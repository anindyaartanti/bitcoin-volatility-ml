import os
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from prefect import flow, task

load_dotenv()

sys.path.insert(0, "/opt/processing")


@task(retries=1, retry_delay_seconds=30)
def validate_minio_data() -> int:
    from minio import Minio

    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    endpoint = endpoint.replace("http://", "").replace("https://", "")
    client = Minio(
        endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        secure=False,
    )

    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=7)
    objects = list(client.list_objects("twitter-raw", recursive=True))
    recent = [o for o in objects if o.last_modified and o.last_modified > cutoff]

    if not recent:
        raise ValueError(
            f"Tidak ada file Parquet baru di MinIO twitter-raw sejak {cutoff.isoformat()}. "
            "Pastikan twitter_ingestion berhasil."
        )

    print(f"Ditemukan {len(recent)} file Parquet baru untuk diproses.")
    return len(recent)


@task
def run_sentiment_pipeline() -> dict:
    from batch_sentiment import run_pipeline

    hours = int(os.getenv("SENTIMENT_LOOKBACK_HOURS", "6"))
    result = run_pipeline(hours=hours)
    return result


@task
def log_audit(result: dict):
    import psycopg2

    tweets = result.get("tweets_processed", 0)
    hours = result.get("hours_updated", 0)
    files = result.get("parquet_files", 0)

    try:
        conn = psycopg2.connect(
            host=os.getenv("APP_DB_HOST", "postgres"),
            port=int(os.getenv("APP_DB_PORT", "5432")),
            dbname=os.getenv("APP_DB_NAME", "btcdb"),
            user=os.getenv("APP_DB_USER", "btcadmin"),
            password=os.getenv("APP_DB_PASSWORD", ""),
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit_log (username, action, table_name, details)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "prefect",
                    "PIPELINE_RUN",
                    "sentiment_hourly",
                    f"sentiment_processing selesai. Tweet: {tweets}, "
                    f"Jam: {hours}, File: {files}.",
                ),
            )
            conn.commit()
        conn.close()
    except Exception as e:
        print(f"Warning: gagal catat audit log: {e}")


@flow(log_prints=True)
def sentiment_processing_flow():
    parquet_count = validate_minio_data()
    result = run_sentiment_pipeline()
    log_audit(result)
    print(f"Sentiment processing selesai: {result}")


if __name__ == "__main__":
    sentiment_processing_flow()
