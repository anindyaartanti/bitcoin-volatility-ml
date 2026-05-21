"""
airflow/dags/twitter_ingestion.py
==================================
DAG Airflow untuk menjalankan scraping Twitter setiap jam
dan menyimpan hasilnya sebagai Parquet di MinIO.

Schedule: setiap jam (0 * * * *)
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Tambahkan path ingestion agar bisa import twitter_batch
sys.path.insert(0, "/opt/airflow/ingestion")

logger = logging.getLogger(__name__)

# ─── Default Args ─────────────────────────────────────────────
default_args = {
    "owner":            "bigdata_team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ─── Query untuk Scraping ─────────────────────────────────────
TWITTER_QUERIES = [
    "bitcoin OR BTC -is:retweet lang:en",
    "#bitcoin OR #BTC -is:retweet lang:en",
    "bitcoin price prediction -is:retweet lang:en",
    "crypto BTC bullish OR bearish -is:retweet lang:en",
]
TWEET_LIMIT_PER_QUERY = int(os.getenv("TWITTER_SCRAPE_LIMIT", "100"))


# ─── Task Functions ───────────────────────────────────────────
def check_dependencies(**context):
    """Pastikan tweet-harvest dan MinIO dapat diakses sebelum scraping."""
    import subprocess
    import shutil

    # Cek Node.js tersedia
    if not shutil.which("node") and not shutil.which("npx"):
        raise EnvironmentError(
            "Node.js / npx tidak ditemukan. "
            "Pastikan sudah install di container Airflow."
        )

    # Cek tweet-harvest bisa dipanggil
    result = subprocess.run(
        ["npx", "tweet-harvest@latest", "--version"],
        capture_output=True, text=True, timeout=30
    )
    logger.info("tweet-harvest version check: %s", result.stdout.strip())

    # Cek MinIO koneksi
    from minio import Minio
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", ""),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        secure=False,
    )
    buckets = [b.name for b in client.list_buckets()]
    logger.info("MinIO buckets tersedia: %s", buckets)

    logger.info("Dependency check lulus.")


def run_twitter_scraping(**context):
    """Jalankan scraping Twitter untuk semua query dan upload ke MinIO."""
    from twitter_batch import run_ingestion

    total = run_ingestion(
        queries=TWITTER_QUERIES,
        limit=TWEET_LIMIT_PER_QUERY,
    )

    # Simpan jumlah tweet ke XCom untuk downstream task
    context["ti"].xcom_push(key="total_tweets", value=total)
    logger.info("Total tweet berhasil discrape dan diupload: %d", total)

    if total == 0:
        raise ValueError(
            "Tidak ada tweet yang berhasil discrape. "
            "Cek TWITTER_AUTH_TOKEN atau query yang digunakan."
        )

    return total


def log_pipeline_audit(**context):
    """Catat event pipeline ke audit_log di PostgreSQL."""
    import psycopg2

    total_tweets = context["ti"].xcom_pull(
        task_ids="scrape_twitter", key="total_tweets"
    ) or 0

    execution_date = context["execution_date"].isoformat()

    try:
        conn = psycopg2.connect(
            host=os.getenv("APP_DB_HOST", "postgres"),
            port=int(os.getenv("APP_DB_PORT", 5432)),
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
                    "airflow",
                    "PIPELINE_RUN",
                    "twitter_posts (MinIO)",
                    f"DAG twitter_ingestion selesai. execution_date={execution_date}, "
                    f"total_tweets={total_tweets}",
                ),
            )
            conn.commit()
        conn.close()
        logger.info("Audit log dicatat.")
    except Exception as e:
        logger.warning("Gagal catat audit log: %s", e)
        # Tidak raise — audit logging tidak boleh gagalkan DAG


# ─── DAG Definition ──────────────────────────────────────────
with DAG(
    dag_id="twitter_ingestion",
    description="Scraping tweet Bitcoin setiap jam → Parquet → MinIO",
    default_args=default_args,
    schedule_interval="0 * * * *",          # setiap jam di menit ke-0
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "twitter", "batch"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_deps = PythonOperator(
        task_id="check_dependencies",
        python_callable=check_dependencies,
    )

    scrape = PythonOperator(
        task_id="scrape_twitter",
        python_callable=run_twitter_scraping,
    )

    audit = PythonOperator(
        task_id="log_audit",
        python_callable=log_pipeline_audit,
    )

    end = EmptyOperator(task_id="end")

    # Alur: start → check_deps → scrape → audit → end
    start >> check_deps >> scrape >> audit >> end
