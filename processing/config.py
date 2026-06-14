"""
processing/config.py
====================
Konfigurasi terpusat untuk semua Spark jobs.
Dibaca oleh stream_processor.py dan batch_sentiment.py.
"""

import os

# ─── Kafka ───────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC", "btc_ticker_raw")

# ─── PostgreSQL ──────────────────────────────────────────────
PG_HOST     = os.getenv("APP_DB_HOST", "postgres")
PG_PORT     = os.getenv("APP_DB_PORT", "5432")
PG_DB       = os.getenv("APP_DB_NAME", "btcdb")
PG_USER     = os.getenv("APP_DB_USER", "btcadmin")
PG_PASSWORD = os.getenv("APP_DB_PASSWORD", "gantiPasswordAman123")
PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

PG_JDBC_PROPERTIES = {
    "user":     PG_USER,
    "password": PG_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

# ─── MinIO / S3A ─────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

CHECKPOINT_BUCKET = "checkpoints"
CHECKPOINT_PATH   = f"s3a://{CHECKPOINT_BUCKET}/spark-streaming"

# ─── Spark packages (dipakai saat spark-submit) ──────────────
SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.postgresql:postgresql:42.7.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])