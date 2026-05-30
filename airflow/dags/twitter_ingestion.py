"""
airflow/dags/twitter_ingestion.py
==================================
DAG: Scrape tweet Bitcoin setiap 6 jam → simpan Parquet ke MinIO.
Setelah sukses, trigger DAG sentiment_processing secara otomatis.

Schedule: setiap 6 jam (0 */6 * * *)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ─── Default args ────────────────────────────────────────────
default_args = {
    "owner": "bigdata-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ─── Task functions ──────────────────────────────────────────
def run_twitter_ingestion(**context):
    """
    Jalankan ingestion/twitter_batch.py sebagai modul Python.
    Dipanggil dari Airflow, bukan subprocess, agar environment sama.
    """
    import sys
    import os

    # Tambah path supaya bisa import modul ingestion
    sys.path.insert(0, "/opt/airflow/ingestion")
    from twitter_batch import run_ingestion  # noqa: E402

    queries = [
        "bitcoin OR BTC -is:retweet lang:en",
        "#bitcoin OR #BTC -is:retweet lang:en",
        "bitcoin price prediction -is:retweet lang:en",
    ]
    limit = int(os.getenv("TWITTER_SCRAPE_LIMIT", "200"))

    total = run_ingestion(queries=queries, limit=limit)
    context["ti"].xcom_push(key="tweet_count", value=total)
    return total


def log_ingestion_result(**context):
    """Catat hasil ingestion ke audit_log PostgreSQL."""
    import os
    import psycopg2

    ti = context["ti"]
    tweet_count = ti.xcom_pull(task_ids="scrape_tweets", key="tweet_count") or 0

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
                    "twitter_raw (MinIO)",
                    f"twitter_ingestion DAG selesai. Total tweet: {tweet_count}. "
                    f"Run ID: {context['run_id']}",
                ),
            )
            conn.commit()
        conn.close()
    except Exception as e:
        # Audit logging tidak boleh gagalkan DAG
        print(f"Warning: gagal catat audit log: {e}")


# ─── DAG Definition ──────────────────────────────────────────
with DAG(
    dag_id="twitter_ingestion",
    description="Scrape tweet Bitcoin → MinIO Parquet setiap 6 jam",
    schedule_interval="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "twitter", "batch"],
    max_active_runs=1,
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_tweets",
        python_callable=run_twitter_ingestion,
        provide_context=True,
    )

    audit_task = PythonOperator(
        task_id="log_audit",
        python_callable=log_ingestion_result,
        provide_context=True,
    )

    trigger_sentiment = TriggerDagRunOperator(
        task_id="trigger_sentiment_processing",
        trigger_dag_id="sentiment_processing",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    scrape_task >> audit_task >> trigger_sentiment