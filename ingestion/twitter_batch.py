"""
ingestion/twitter_batch.py
==========================
Scraping tweet Bitcoin menggunakan tweet-harvest (CLI wrapper untuk
Twitter/X tanpa API key resmi), lalu simpan sebagai Parquet ke MinIO.

Dipanggil oleh Airflow DAG twitter_ingestion setiap jam.

Dependensi:
    - Node.js (untuk menjalankan tweet-harvest)
    - tweet-harvest (npm install -g tweet-harvest)
    - pandas, pyarrow, minio, python-dotenv

Cara install tweet-harvest di container:
    npm install -g tweet-harvest

Cara pakai manual (tes tanpa Airflow):
    python twitter_batch.py --query "bitcoin OR BTC" --limit 100
"""

import argparse
import logging
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv()

# ─── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("twitter_batch")

# ─── Konfigurasi dari environment ───────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET     = "twitter-raw"

# Auth token Twitter/X (dari cookie 'auth_token')
# Didapat dari browser DevTools > Application > Cookies > twitter.com
TWITTER_AUTH_TOKEN = os.getenv("TWITTER_AUTH_TOKEN", "")

# ─── Query default untuk scraping ───────────────────────────
DEFAULT_QUERIES = [
    "bitcoin OR BTC -is:retweet lang:en",
    "#bitcoin OR #BTC -is:retweet lang:en",
    "bitcoin price -is:retweet lang:en",
]
DEFAULT_LIMIT = int(os.getenv("TWITTER_SCRAPE_LIMIT", "100"))


# ─── MinIO Client ────────────────────────────────────────────
def get_minio_client(endpoint: str = None) -> Minio:
    raw = endpoint or MINIO_ENDPOINT
    ep = raw.replace("http://", "").replace("https://", "")
    secure = raw.startswith("https://")
    return Minio(
        ep,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
    )


def ensure_bucket(client: Minio, bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info("Bucket '%s' dibuat.", bucket)


# ─── Scraping dengan tweet-harvest ───────────────────────────
def scrape_tweets(query: str, limit: int, work_dir: str) -> int:
    """
    Jalankan tweet-harvest via subprocess (v2.7+).
    Menghasilkan file CSV di {work_dir}/tweets-data/.
    Mengembalikan jumlah tweet yang di-scrape.
    """
    if not TWITTER_AUTH_TOKEN:
        raise ValueError(
            "TWITTER_AUTH_TOKEN tidak diset. "
            "Ambil dari cookie 'auth_token' di browser saat login ke Twitter/X."
        )

    cmd = [
        "npx", "--yes", "tweet-harvest",
        "--token",           TWITTER_AUTH_TOKEN,
        "--search-keyword",  query,
        "--limit",           str(limit),
        "--export-format",   "csv",
        "--tab",             "LATEST",
    ]

    logger.info("Menjalankan tweet-harvest: query='%s', limit=%d", query, limit)
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=300,
        cwd=work_dir,
    )

    if result.returncode != 0:
        logger.error("tweet-harvest gagal:\nSTDOUT: %s\nSTDERR: %s", result.stdout, result.stderr)
        raise RuntimeError(f"tweet-harvest exit code {result.returncode}")

    logger.info("tweet-harvest selesai:\n%s", result.stdout[:500])

    # Cari file CSV yang dihasilkan di {work_dir}/tweets-data/
    tweets_dir = Path(work_dir) / "tweets-data"
    if not tweets_dir.exists():
        logger.warning("Direktori tweets-data tidak ditemukan.")
        return 0

    csv_files = sorted(tweets_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not csv_files:
        logger.warning("Tidak ada file CSV ditemukan di %s", tweets_dir)
        return 0

    df = pd.read_csv(csv_files[0])
    count = len(df)
    logger.info("Berhasil scrape %d tweet untuk query: '%s'", count, query)
    return count


# ─── Transformasi CSV → DataFrame ────────────────────────────
def csv_to_dataframe(csv_dir: str, query: str, scrape_time: datetime) -> pd.DataFrame:
    """
    Baca CSV hasil tweet-harvest v2.7+ dan normalisasi ke DataFrame.
    """
    tweets_dir = Path(csv_dir) / "tweets-data"
    if not tweets_dir.exists():
        return pd.DataFrame()

    csv_files = sorted(tweets_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not csv_files:
        return pd.DataFrame()

    raw = pd.read_csv(csv_files[0])

    if raw.empty:
        return pd.DataFrame()

    # Mapping kolom CSV tweet-harvest ke skema internal
    df = pd.DataFrame({
        "tweet_id":        raw["id_str"].astype(str),
        "text":            raw["full_text"].fillna(""),
        "username":        raw.get("username", "").astype(str),
        "user_followers":  0,
        "created_at":      pd.to_datetime(raw["created_at"], errors="coerce", utc=True),
        "retweet_count":   pd.to_numeric(raw["retweet_count"], errors="coerce").fillna(0).astype(int),
        "favorite_count":  pd.to_numeric(raw["favorite_count"], errors="coerce").fillna(0).astype(int),
        "lang":            raw.get("lang", "").astype(str),
        "query":           query,
        "scrape_time":     scrape_time.isoformat(),
    })

    df["scrape_time"]   = pd.to_datetime(df["scrape_time"], utc=True)

    # Hapus duplikat tweet_id
    df = df.drop_duplicates(subset=["tweet_id"])
    df = df[df["tweet_id"] != ""]

    return df


# ─── Upload ke MinIO ─────────────────────────────────────────
def upload_parquet_to_minio(df: pd.DataFrame, client: Minio, hour_str: str, query_idx: int):
    """
    Simpan DataFrame sebagai Parquet di MinIO.
    Nama file: twitter_posts_YYYYMMDD_HH_<idx>.parquet
    """
    if df.empty:
        logger.warning("DataFrame kosong, tidak ada yang diupload untuk query index %d.", query_idx)
        return None

    object_name = f"twitter_posts_{hour_str}_{query_idx:02d}.parquet"

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        df.to_parquet(tmp.name, engine="pyarrow", index=False)
        tmp_path = tmp.name

    try:
        client.fput_object(
            MINIO_BUCKET,
            object_name,
            tmp_path,
            content_type="application/octet-stream",
        )
        logger.info("Berhasil upload %s ke MinIO bucket '%s' (%d baris)", object_name, MINIO_BUCKET, len(df))
        return object_name
    except S3Error as e:
        logger.error("Gagal upload ke MinIO: %s", e)
        raise
    finally:
        Path(tmp_path).unlink(missing_ok=True)


# ─── Catat metadata ke PostgreSQL ────────────────────────────
def log_metadata(object_name: str, record_count: int, scrape_time: datetime):
    """
    Tulis metadata scraping ke tabel metadata_table di PostgreSQL.
    Menggunakan psycopg2 langsung (tidak butuh Spark di Fase 1).
    """
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv("APP_DB_HOST", "localhost"),
            port=int(os.getenv("APP_DB_PORT", 5432)),
            dbname=os.getenv("APP_DB_NAME", "btcdb"),
            user=os.getenv("APP_DB_USER", "btcadmin"),
            password=os.getenv("APP_DB_PASSWORD", ""),
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO metadata_table
                    (dataset_name, source, location, record_count, run_timestamp, pipeline_name, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    "twitter_posts",
                    "Twitter/X via tweet-harvest",
                    f"s3://{MINIO_BUCKET}/{object_name}",
                    record_count,
                    scrape_time,
                    "twitter_ingestion",
                    "success",
                ),
            )
            conn.commit()
        conn.close()
        logger.info("Metadata dicatat ke PostgreSQL untuk: %s", object_name)
    except Exception as e:
        # Metadata logging tidak boleh hentikan pipeline
        logger.warning("Gagal catat metadata ke PostgreSQL: %s", e)


# ─── Main Function ────────────────────────────────────────────
def run_ingestion(queries: list = None, limit: int = DEFAULT_LIMIT, minio_endpoint: str = None):
    """
    Fungsi utama yang dipanggil oleh Airflow DAG atau langsung.
    """
    if queries is None:
        queries = DEFAULT_QUERIES

    scrape_time = datetime.now(tz=timezone.utc)
    hour_str    = scrape_time.strftime("%Y%m%d_%H")

    minio_client = get_minio_client(endpoint=minio_endpoint)
    ensure_bucket(minio_client, MINIO_BUCKET)

    all_dfs    = []
    total_rows = 0

    for idx, query in enumerate(queries):
        with tempfile.TemporaryDirectory() as tmp_dir:
            try:
                count = scrape_tweets(query, limit, tmp_dir)
                if count == 0:
                    logger.warning("Tidak ada tweet untuk query: '%s'", query)
                    continue

                df = csv_to_dataframe(tmp_dir, query, scrape_time)
                if df.empty:
                    continue

                object_name = upload_parquet_to_minio(df, minio_client, hour_str, idx)
                if object_name:
                    log_metadata(object_name, len(df), scrape_time)
                    total_rows += len(df)
                    all_dfs.append(df)

            except Exception as e:
                logger.error("Error pada query '%s': %s", query, e)
                # Lanjut ke query berikutnya

    logger.info(
        "Ingestion selesai: %d query diproses, total %d tweet, jam=%s",
        len(queries), total_rows, hour_str,
    )
    return total_rows


# ─── CLI Entry Point ─────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Twitter batch scraper → MinIO")
    parser.add_argument("--query",          type=str,  default=None,                    help="Query tunggal (opsional, override default)")
    parser.add_argument("--limit",          type=int,  default=DEFAULT_LIMIT,           help="Jumlah tweet per query")
    parser.add_argument("--minio-endpoint", type=str,  default=None,                    help="MinIO endpoint, misal localhost:9000 (override .env)")
    args = parser.parse_args()

    queries = [args.query] if args.query else None
    run_ingestion(queries=queries, limit=args.limit, minio_endpoint=args.minio_endpoint)
