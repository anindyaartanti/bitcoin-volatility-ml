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
import json
import logging
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from minio import Minio
from minio.error import S3Error

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
def get_minio_client() -> Minio:
    secure = not MINIO_ENDPOINT.startswith("localhost")
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
    )


def ensure_bucket(client: Minio, bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info("Bucket '%s' dibuat.", bucket)


# ─── Scraping dengan tweet-harvest ───────────────────────────
def scrape_tweets(query: str, limit: int, output_path: str) -> int:
    """
    Jalankan tweet-harvest via subprocess.
    Menghasilkan file JSON di output_path.
    Mengembalikan jumlah tweet yang di-scrape.
    """
    if not TWITTER_AUTH_TOKEN:
        raise ValueError(
            "TWITTER_AUTH_TOKEN tidak diset. "
            "Ambil dari cookie 'auth_token' di browser saat login ke Twitter/X."
        )

    cmd = [
        "npx", "--yes", "tweet-harvest@latest",
        "--query",      query,
        "--limit",      str(limit),
        "--token",      TWITTER_AUTH_TOKEN,
        "--output",     output_path,
        "--type",       "json",
    ]

    logger.info("Menjalankan tweet-harvest: query='%s', limit=%d", query, limit)
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=300,   # maks 5 menit per query
    )

    if result.returncode != 0:
        logger.error("tweet-harvest gagal:\nSTDOUT: %s\nSTDERR: %s", result.stdout, result.stderr)
        raise RuntimeError(f"tweet-harvest exit code {result.returncode}")

    logger.info("tweet-harvest selesai:\n%s", result.stdout[:500])

    # Hitung jumlah tweet yang berhasil diambil
    if Path(output_path).exists():
        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        count = len(data) if isinstance(data, list) else 0
        logger.info("Berhasil scrape %d tweet untuk query: '%s'", count, query)
        return count
    return 0


# ─── Transformasi JSON → DataFrame ───────────────────────────
def json_to_dataframe(json_path: str, query: str, scrape_time: datetime) -> pd.DataFrame:
    """
    Baca JSON hasil tweet-harvest dan normalisasi ke DataFrame.
    tweet-harvest menghasilkan array of tweet objects.
    """
    with open(json_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if not raw:
        return pd.DataFrame()

    rows = []
    for tweet in raw:
        # tweet-harvest field names (dapat bervariasi per versi)
        row = {
            "tweet_id":        str(tweet.get("id_str") or tweet.get("id", "")),
            "text":            tweet.get("full_text") or tweet.get("text", ""),
            "username":        tweet.get("user", {}).get("screen_name", ""),
            "user_followers":  tweet.get("user", {}).get("followers_count", 0),
            "created_at":      tweet.get("created_at", ""),
            "retweet_count":   tweet.get("retweet_count", 0),
            "favorite_count":  tweet.get("favorite_count", 0),
            "lang":            tweet.get("lang", ""),
            "query":           query,
            "scrape_time":     scrape_time.isoformat(),
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # Konversi tipe
    df["created_at"]    = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    df["scrape_time"]   = pd.to_datetime(df["scrape_time"], utc=True)
    df["retweet_count"] = pd.to_numeric(df["retweet_count"], errors="coerce").fillna(0).astype(int)
    df["favorite_count"]= pd.to_numeric(df["favorite_count"], errors="coerce").fillna(0).astype(int)

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
def run_ingestion(queries: list = None, limit: int = DEFAULT_LIMIT):
    """
    Fungsi utama yang dipanggil oleh Airflow DAG atau langsung.
    """
    if queries is None:
        queries = DEFAULT_QUERIES

    scrape_time = datetime.now(tz=timezone.utc)
    hour_str    = scrape_time.strftime("%Y%m%d_%H")

    minio_client = get_minio_client()
    ensure_bucket(minio_client, MINIO_BUCKET)

    all_dfs    = []
    total_rows = 0

    for idx, query in enumerate(queries):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp_json:
            tmp_json_path = tmp_json.name

        try:
            count = scrape_tweets(query, limit, tmp_json_path)
            if count == 0:
                logger.warning("Tidak ada tweet untuk query: '%s'", query)
                continue

            df = json_to_dataframe(tmp_json_path, query, scrape_time)
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
        finally:
            Path(tmp_json_path).unlink(missing_ok=True)

    logger.info(
        "Ingestion selesai: %d query diproses, total %d tweet, jam=%s",
        len(queries), total_rows, hour_str,
    )
    return total_rows


# ─── CLI Entry Point ─────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Twitter batch scraper → MinIO")
    parser.add_argument("--query",  type=str,  default=None,          help="Query tunggal (opsional, override default)")
    parser.add_argument("--limit",  type=int,  default=DEFAULT_LIMIT, help="Jumlah tweet per query")
    args = parser.parse_args()

    queries = [args.query] if args.query else None
    run_ingestion(queries=queries, limit=args.limit)
