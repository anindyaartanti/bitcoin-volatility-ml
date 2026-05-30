"""
airflow/dags/sentiment_processing.py
======================================
DAG: Spark Batch — baca Parquet dari MinIO → VADER sentiment scoring
     → agregasi per jam → simpan ke PostgreSQL sentiment_hourly.

Dipanggil otomatis oleh twitter_ingestion DAG via TriggerDagRunOperator.
Bisa juga dijalankan manual dari Airflow UI.

Schedule: None (dipicu oleh twitter_ingestion)
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ─── Default args ────────────────────────────────────────────
default_args = {
    "owner": "bigdata-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

# ─── Spark jars untuk S3A + PostgreSQL JDBC ─────────────────
SPARK_JARS = ",".join([
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.7.1",
])


# ─── Task: validasi data sebelum Spark ───────────────────────
def validate_minio_data(**context):
    """
    Cek apakah ada file Parquet baru di MinIO twitter-raw
    dari 6 jam terakhir. Gagal jika tidak ada.
    """
    from minio import Minio
    from datetime import timezone

    client = Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", ""),
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
    context["ti"].xcom_push(key="parquet_count", value=len(recent))


# ─── Task: log hasil ke audit ────────────────────────────────
def log_sentiment_result(**context):
    """Catat hasil Spark batch ke audit_log."""
    import psycopg2

    ti = context["ti"]
    parquet_count = ti.xcom_pull(task_ids="validate_input", key="parquet_count") or 0

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
                    "sentiment_hourly",
                    f"sentiment_processing DAG selesai. "
                    f"File diproses: {parquet_count}. Run ID: {context['run_id']}",
                ),
            )
            conn.commit()
        conn.close()
    except Exception as e:
        print(f"Warning: gagal catat audit log: {e}")


# ─── DAG Definition ──────────────────────────────────────────
with DAG(
    dag_id="sentiment_processing",
    description="Spark Batch: MinIO Parquet → VADER → sentiment_hourly PostgreSQL",
    schedule_interval=None,   # triggered oleh twitter_ingestion
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["processing", "spark", "sentiment", "batch"],
    max_active_runs=1,
) as dag:

    validate_task = PythonOperator(
        task_id="validate_input",
        python_callable=validate_minio_data,
        provide_context=True,
    )

    spark_task = SparkSubmitOperator(
        task_id="run_spark_sentiment",
        conn_id="spark_default",          # Airflow Connection: spark://spark-master:7077
        application="/opt/airflow/processing/batch_sentiment.py",
        name="btc-sentiment-batch",
        packages=SPARK_JARS,
        conf={
            # MinIO / S3A
            "spark.hadoop.fs.s3a.endpoint":              "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key":            "minioadmin",
            "spark.hadoop.fs.s3a.secret.key":            "minioadmin123",
            "spark.hadoop.fs.s3a.path.style.access":     "true",
            "spark.hadoop.fs.s3a.impl":                  "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled":"false",
            # Resource
            "spark.executor.memory":                     "1g",
            "spark.driver.memory":                       "1g",
        },
        env_vars={
            "APP_DB_HOST":     "postgres",
            "APP_DB_PORT":     "5432",
            "APP_DB_NAME":     "btcdb",
            "APP_DB_USER":     "btcadmin",
            "APP_DB_PASSWORD": "{{ var.value.get('APP_DB_PASSWORD', 'gantiPasswordAman123') }}",
        },
        verbose=False,
    )

    audit_task = PythonOperator(
        task_id="log_audit",
        python_callable=log_sentiment_result,
        provide_context=True,
    )

    validate_task >> spark_task >> audit_task