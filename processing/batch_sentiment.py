import argparse
import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("batch_sentiment")


def get_minio_client(endpoint: str = None):
    ep = (endpoint or os.getenv("MINIO_ENDPOINT", "http://minio:9000")).replace("http://", "").replace("https://", "")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    secure = (endpoint or os.getenv("MINIO_ENDPOINT", "")).startswith("https")
    return Minio(ep, access_key=access_key, secret_key=secret_key, secure=secure)


def get_db_conn():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("APP_DB_HOST", "postgres"),
        port=int(os.getenv("APP_DB_PORT", "5432")),
        dbname=os.getenv("APP_DB_NAME", "btcdb"),
        user=os.getenv("APP_DB_USER", "btcadmin"),
        password=os.getenv("APP_DB_PASSWORD", "gantiPasswordAman123"),
    )


def get_finvader_scores(text: str) -> dict:
    from finvader import finvader

    try:
        compound = finvader(text, use_sentibignomics=True, use_henry=True, indicator="compound")
        pos = finvader(text, use_sentibignomics=True, use_henry=True, indicator="pos")
        neg = finvader(text, use_sentibignomics=True, use_henry=True, indicator="neg")
        neu = finvader(text, use_sentibignomics=True, use_henry=True, indicator="neu")
        return {"compound": compound, "pos": pos, "neg": neg, "neu": neu}
    except Exception as e:
        logger.warning("FinVADER error pada text (len=%d): %s", len(text), e)
        return {"compound": 0.0, "pos": 0.0, "neg": 0.0, "neu": 1.0}


def list_recent_parquet(client: Minio, bucket: str, hours: int) -> list:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=hours)
    objects = list(client.list_objects(bucket, recursive=True))
    recent = [o for o in objects if o.last_modified and o.last_modified > cutoff]
    recent.sort(key=lambda o: o.last_modified)
    logger.info("Ditemukan %d file Parquet dalam %d jam terakhir", len(recent), hours)
    return recent


def download_parquet(client: Minio, bucket: str, object_name: str) -> str:
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    client.fget_object(bucket, object_name, tmp.name)
    return tmp.name


def process_dataframe(df: pd.DataFrame, scrape_time: datetime) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    results = []
    for _, row in df.iterrows():
        text = str(row.get("text", "") or "")
        if not text.strip():
            continue

        scores = get_finvader_scores(text)

        results.append({
            "tweet_id": str(row.get("tweet_id", "")),
            "tweet_text": text[:500],
            "tweet_created_at": row.get("created_at"),
            "scrape_time": scrape_time,
            "sentiment_score": scores["compound"],
            "positive": scores["pos"],
            "negative": scores["neg"],
            "neutral": scores["neu"],
        })

    return pd.DataFrame(results)


def insert_sentiment_raw(conn, df: pd.DataFrame):
    if df.empty:
        return 0

    count = 0
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            try:
                cur.execute(
                    """
                    INSERT INTO sentiment_raw
                        (tweet_id, tweet_text, tweet_created_at, scrape_time,
                         sentiment_score, positive, negative, neutral)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tweet_id) DO NOTHING
                    """,
                    (
                        row["tweet_id"],
                        row["tweet_text"],
                        row["tweet_created_at"],
                        row["scrape_time"],
                        row["sentiment_score"],
                        row["positive"],
                        row["negative"],
                        row["neutral"],
                    ),
                )
                count += 1
            except Exception as e:
                logger.warning("Gagal insert tweet %s: %s", row.get("tweet_id", "?"), e)
        conn.commit()
    return count


def upsert_sentiment_hourly(conn, scrape_time: datetime):
    hour_start = scrape_time.replace(minute=0, second=0, microsecond=0)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sentiment_hourly
                (hour_timestamp, avg_sentiment, std_sentiment,
                 mention_count, positive_count, negative_count, neutral_count)
            SELECT
                DATE_TRUNC('hour', scrape_time) AS hour_ts,
                AVG(sentiment_score),
                COALESCE(STDDEV(sentiment_score), 0),
                COUNT(*)                                               AS mention_count,
                COUNT(*) FILTER (WHERE sentiment_score >= 0.05)        AS positive_count,
                COUNT(*) FILTER (WHERE sentiment_score <= -0.05)       AS negative_count,
                COUNT(*) FILTER (WHERE sentiment_score > -0.05
                                 AND sentiment_score < 0.05)           AS neutral_count
            FROM sentiment_raw
            WHERE scrape_time >= %s
              AND scrape_time < %s + INTERVAL '1 hour'
            GROUP BY hour_ts
            ON CONFLICT (hour_timestamp)
            DO UPDATE SET
                avg_sentiment   = EXCLUDED.avg_sentiment,
                std_sentiment   = EXCLUDED.std_sentiment,
                mention_count   = EXCLUDED.mention_count,
                positive_count  = EXCLUDED.positive_count,
                negative_count  = EXCLUDED.negative_count,
                neutral_count   = EXCLUDED.neutral_count
            """,
            (hour_start, hour_start),
        )
        updated = cur.rowcount
        conn.commit()
        return updated


def log_metadata(conn, tweets_processed: int, hours_updated: int, scrape_time: datetime):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO metadata_table
                (dataset_name, source, location, record_count, run_timestamp, pipeline_name, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                "sentiment_result",
                "FinVADER on Twitter/X Parquet",
                "postgresql:btcdb.sentiment_raw",
                tweets_processed,
                scrape_time,
                "sentiment_processing",
                "success",
            ),
        )
        conn.commit()


def run_pipeline(hours: int = 6, minio_endpoint: str = None) -> dict:
    scrape_time = datetime.now(tz=timezone.utc)
    minio_client = get_minio_client(endpoint=minio_endpoint)
    bucket = os.getenv("MINIO_TWITTER_BUCKET", "twitter-raw")

    files = list_recent_parquet(minio_client, bucket, hours)
    if not files:
        logger.warning("Tidak ada file parquet untuk diproses dalam %d jam terakhir", hours)
        return {"tweets_processed": 0, "hours_updated": 0, "parquet_files": 0}

    conn = get_db_conn()
    total_tweets = 0

    try:
        for obj in files:
            tmp_path = None
            try:
                tmp_path = download_parquet(minio_client, bucket, obj.object_name)
                df = pd.read_parquet(tmp_path)
                logger.info("Memproses %s: %d baris", obj.object_name, len(df))

                results_df = process_dataframe(df, scrape_time)
                inserted = insert_sentiment_raw(conn, results_df)
                total_tweets += inserted
                logger.info("  -> %d tweet baru dimasukkan ke sentiment_raw", inserted)

            except Exception as e:
                logger.error("Gagal memproses %s: %s", obj.object_name, e)
            finally:
                if tmp_path:
                    Path(tmp_path).unlink(missing_ok=True)

        hours_updated = 0
        if total_tweets > 0:
            hours_updated = upsert_sentiment_hourly(conn, scrape_time)
            log_metadata(conn, total_tweets, hours_updated, scrape_time)
            logger.info(
                "Selesai: %d tweet diproses, %d jam di-update",
                total_tweets, hours_updated,
            )
        else:
            logger.warning("Tidak ada tweet baru yang diproses.")

    finally:
        conn.close()

    return {
        "tweets_processed": total_tweets,
        "hours_updated": hours_updated,
        "parquet_files": len(files),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FinVADER sentiment batch dari MinIO → PostgreSQL")
    parser.add_argument("--hours",          type=int,  default=6,   help="Jumlah jam lookback untuk file Parquet")
    parser.add_argument("--minio-endpoint", type=str,  default=None, help="MinIO endpoint, misal localhost:9000 (override .env)")
    args = parser.parse_args()

    result = run_pipeline(hours=args.hours, minio_endpoint=args.minio_endpoint)
    print(f"Result: {result}")
