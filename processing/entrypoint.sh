#!/bin/bash
# docker/spark-streaming/entrypoint.sh
# Tunggu Kafka dan Spark Master siap, lalu submit job.

set -e

KAFKA_HOST="${KAFKA_HOST:-kafka}"
KAFKA_PORT="${KAFKA_PORT:-29092}"
SPARK_MASTER="${SPARK_MASTER_URL:-spark://spark-master:7077}"
JOB_PATH="/opt/spark/jobs/stream_processor.py"

echo "=== Spark Streaming Job Entrypoint ==="

# ── Tunggu Kafka ────────────────────────────────────────────
echo "Menunggu Kafka di ${KAFKA_HOST}:${KAFKA_PORT}..."
until bash -c "cat /dev/null > /dev/tcp/${KAFKA_HOST}/${KAFKA_PORT}" 2>/dev/null; do
    echo "  Kafka belum siap, tunggu 5 detik..."
    sleep 5
done
echo "Kafka siap."

# ── Tunggu Spark Master ─────────────────────────────────────
echo "Menunggu Spark Master..."
until bash -c "cat /dev/null > /dev/tcp/spark-master/7077" 2>/dev/null; do
    echo "  Spark Master belum siap, tunggu 5 detik..."
    sleep 5
done
echo "Spark Master siap."

# ── Tunggu PostgreSQL ───────────────────────────────────────
echo "Menunggu PostgreSQL..."
until bash -c "cat /dev/null > /dev/tcp/${APP_DB_HOST:-postgres}/5432" 2>/dev/null; do
    echo "  PostgreSQL belum siap, tunggu 5 detik..."
    sleep 5
done
echo "PostgreSQL siap."

# ── Tunggu MinIO ────────────────────────────────────────────
echo "Menunggu MinIO..."
until bash -c "cat /dev/null > /dev/tcp/minio/9000" 2>/dev/null; do
    echo "  MinIO belum siap, tunggu 5 detik..."
    sleep 5
done
echo "MinIO siap."

echo "Installing dependency..."
pip install psycopg2-binary pybreaker requests

echo "Semua dependency siap. Menjalankan spark-submit..."

exec /opt/spark/bin/spark-submit \
    --master "${SPARK_MASTER}" \
    --deploy-mode client \
    --name "btc-stream-processor" \
    --packages \
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.postgresql:postgresql:42.7.1,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262" \
    --conf "spark.executor.memory=1g" \
    --conf "spark.driver.memory=1g" \
    --conf "spark.sql.session.timeZone=UTC" \
    --conf "spark.streaming.stopGracefullyOnShutdown=true" \
    "${JOB_PATH}"