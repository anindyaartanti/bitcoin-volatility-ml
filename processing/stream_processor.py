"""
processing/stream_processor.py
================================
PySpark Structured Streaming: Kafka @trade → tumbling window 1m → btc_ohlc_1m
- Checkpoint ke s3a://checkpoints/spark-streaming/
- Tulis via PgBouncer (transaction pooling)
- pybreaker circuit breaker di setiap PostgreSQL write
- Log pipeline_lineage setelah setiap micro-batch
- XGBoost model cache fallback /tmp/xgb_model_cache
"""

import logging
import os
import sys

import pybreaker
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    ArrayType,
)

sys.path.insert(0, os.path.dirname(__file__))
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("stream_processor")

# ─── Env ─────────────────────────────────────────────────────
PGBOUNCER_HOST  = os.getenv("PGBOUNCER_HOST", "pgbouncer")
PGBOUNCER_PORT  = int(os.getenv("PGBOUNCER_PORT", "6432"))
PG_DIRECT_HOST  = os.getenv("APP_DB_HOST", "postgres")
PG_DIRECT_PORT  = int(os.getenv("APP_DB_PORT", "5432"))
PG_DB           = os.getenv("APP_DB_NAME", "btcdb")
PG_USER         = os.getenv("APP_DB_USER", "btcadmin")
PG_PASSWORD     = os.getenv("APP_DB_PASSWORD", "")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "btc_ticker_dlq")
MLFLOW_URI      = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
CHECKPOINT_PATH = "s3a://checkpoints/spark-streaming/"
XGB_CACHE_PATH  = "/tmp/xgb_model_cache"
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT   = os.getenv("TELEGRAM_CHAT_ID", "")

# ─── Schema pesan @trade dari Binance ────────────────────────
TRADE_SCHEMA = StructType([
    StructField("event_time", LongType(),   True),  # ms epoch
    StructField("symbol",     StringType(), True),
    StructField("trade_id",   LongType(),   True),
    StructField("price",      StringType(), True),  # cast ke double
    StructField("quantity",   StringType(), True),
    StructField("is_buyer_mm", StringType(), True),
])

# ─── Circuit breaker ─────────────────────────────────────────
def _on_circuit_open(cb):
    msg = f"[stream_processor] Circuit breaker OPEN: PostgreSQL write gagal {cb.fail_counter}x"
    logger.error(msg)
    _send_telegram(msg)
    _log_audit_direct(msg)


pg_breaker = pybreaker.CircuitBreaker(
    fail_max=5,
    reset_timeout=60,
    listeners=[pybreaker.CircuitBreakerListener()],
)
# Override state change listener
pg_breaker.add_listeners(type(
    "_Listener", (pybreaker.CircuitBreakerListener,),
    {"state_change": staticmethod(lambda cb, old, new: _on_circuit_open(cb) if str(new) == "open" else None)}
)())


def _send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": message},
            timeout=5,
        )
    except Exception:
        pass


def _pg_conn(host: str, port: int):
    import psycopg2
    return psycopg2.connect(
        host=host, port=port, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )


def _log_audit_direct(detail: str):
    """Fallback direct connection ke PostgreSQL saat circuit open."""
    try:
        conn = _pg_conn(PG_DIRECT_HOST, PG_DIRECT_PORT)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO audit_log (table_name, operation, new_data, changed_by) "
                "VALUES ('btc_ohlc_1m', 'INSERT', %s::jsonb, 'spark_streaming')",
                (f'{{"detail": "{detail}"}}',),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("Fallback audit log gagal: %s", e)


# ─── XGBoost model cache ─────────────────────────────────────
def load_xgb_model():
    """Load dari MLflow, cache ke /tmp. Fallback ke cache jika MLflow down."""
    import pickle
    cache = XGB_CACHE_PATH + "/model.pkl"
    try:
        import mlflow.xgboost
        mlflow.set_tracking_uri(MLFLOW_URI)
        client = mlflow.tracking.MlflowClient()
        mv = client.get_latest_versions("btc_volatility_xgb", stages=["Production"])
        if mv:
            model = mlflow.xgboost.load_model(f"runs:/{mv[0].run_id}/model")
            os.makedirs(XGB_CACHE_PATH, exist_ok=True)
            with open(cache, "wb") as f:
                pickle.dump(model, f)
            logger.info("XGBoost model loaded from MLflow dan dicache.")
            return model
    except Exception as e:
        logger.warning("MLflow tidak tersedia (%s), mencoba cache...", e)
    if os.path.exists(cache):
        with open(cache, "rb") as f:
            logger.info("XGBoost model loaded dari cache.")
            return pickle.load(f)
    logger.warning("XGBoost model tidak tersedia.")
    return None


# ─── SparkSession ─────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("btc-stream-processor")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.streaming.stopGracefullyOnShutdown",   "true")
        .config("spark.sql.session.timeZone",                 "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─── UDF: volatility = stddev of per-trade returns ───────────
@F.udf(DoubleType())
def calc_volatility(prices):
    if not prices or len(prices) < 2:
        return None
    try:
        arr = [float(p) for p in prices]
        returns = [(arr[i] - arr[i-1]) / arr[i-1] for i in range(1, len(arr)) if arr[i-1] != 0]
        if not returns:
            return None
        mean = sum(returns) / len(returns)
        variance = sum((r - mean) ** 2 for r in returns) / len(returns)
        return float(variance ** 0.5)
    except Exception:
        return None


# ─── Streaming pipeline ───────────────────────────────────────
def build_stream(spark: SparkSession):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe",               KAFKA_TOPIC_RAW)
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        .option("maxOffsetsPerTrigger",    "2000")
        .load()
    )

    parsed = (
        raw.select(
            F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("d")
        )
        .select(
            # event_time: ms epoch → TimestampType
            (F.col("d.event_time") / 1000).cast("timestamp").alias("event_time"),
            F.col("d.price").cast(DoubleType()).alias("price"),
            F.col("d.quantity").cast(DoubleType()).alias("quantity"),
        )
        .filter(F.col("price").isNotNull() & F.col("event_time").isNotNull())
    )

    windowed = (
        parsed
        .withWatermark("event_time", "1 minute")
        .groupBy(F.window(F.col("event_time"), "1 minute"))
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("quantity").alias("volume"),
            F.count("price").cast("integer").alias("trade_count"),
            calc_volatility(F.collect_list("price")).alias("volatility"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "open", "high", "low", "close", "volume", "trade_count", "volatility",
        )
    )

    return windowed


# ─── ForeachBatch: write via PgBouncer + lineage ──────────────
def write_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        logger.info("Batch %d kosong.", batch_id)
        return

    rows = batch_df.toPandas()
    rows_written = 0
    rows_rejected = 0

    @pg_breaker
    def _write(conn):
        nonlocal rows_written, rows_rejected
        with conn.cursor() as cur:
            for _, row in rows.iterrows():
                cur.execute(
                    """
                    INSERT INTO btc_ohlc_1m
                        (window_start, window_end, open, high, low, close,
                         volume, trade_count, volatility)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (window_start) DO NOTHING
                    """,
                    (
                        row["window_start"].to_pydatetime(),
                        row["window_end"].to_pydatetime(),
                        float(row["open"]),
                        float(row["high"]),
                        float(row["low"]),
                        float(row["close"]),
                        float(row["volume"]),
                        int(row["trade_count"]),
                        float(row["volatility"]) if row["volatility"] is not None else None,
                    ),
                )
                if cur.rowcount > 0:
                    rows_written += 1
                else:
                    rows_rejected += 1
        conn.commit()

    try:
        conn = _pg_conn(PGBOUNCER_HOST, PGBOUNCER_PORT)
        _write(conn)
        conn.close()
        logger.info("Batch %d: %d rows written, %d rejected.", batch_id, rows_written, rows_rejected)
    except pybreaker.CircuitBreakerError:
        logger.error("Circuit breaker OPEN — skipping batch %d", batch_id)
        return
    except Exception as e:
        logger.error("Batch %d write error: %s", batch_id, e)
        raise

    # ── Log pipeline_lineage ──────────────────────────────────
    try:
        import json
        from datetime import datetime, timezone
        conn = _pg_conn(PGBOUNCER_HOST, PGBOUNCER_PORT)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_lineage
                    (pipeline_name, source, target_table, rows_processed,
                     rows_rejected, quality_status, started_at, finished_at, params)
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), %s)
                """,
                (
                    "spark_streaming",
                    KAFKA_TOPIC_RAW,
                    "btc_ohlc_1m",
                    rows_written,
                    rows_rejected,
                    "ok",
                    json.dumps({"window": "1 minute", "batch_id": batch_id}),
                ),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Gagal log pipeline_lineage: %s", e)


# ─── Main ─────────────────────────────────────────────────────
def main():
    logger.info("Memulai Spark Structured Streaming job...")
    load_xgb_model()   # preload — cache jika MLflow tersedia

    spark = create_spark_session()
    windowed = build_stream(spark)

    query = (
        windowed
        .writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Streaming query started. Menunggu data dari Kafka...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
