"""
processing/stream_processor.py
================================
Spark Structured Streaming: Kafka → parse kline → OHLC + rolling volatility
→ PostgreSQL (btc_ohlc_1m)

Scope sekarang (Fase A):
  - Baca topic btc_ticker_raw dari Kafka
  - Filter hanya event_type == "kline" dan is_closed == true
  - Tulis OHLC per candle ke tabel btc_ohlc_1m
  - Hitung rolling volatility 5 candle terakhir via window function

Nanti ditambah (Fase B):
  - Join sentimen dari sentiment_hourly
  - Inference XGBoost → tulis ke predictions
"""

import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ─── Path agar bisa import config dari folder yang sama ──────
sys.path.insert(0, os.path.dirname(__file__))
from config import (
    CHECKPOINT_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    PG_JDBC_PROPERTIES,
    PG_JDBC_URL,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("stream_processor")


# ─── Schema pesan kline dari Binance (via Kafka) ─────────────
KLINE_SCHEMA = StructType([
    StructField("event_type",  StringType(),  True),
    StructField("event_time",  StringType(),  True),
    StructField("symbol",      StringType(),  True),
    StructField("interval",    StringType(),  True),
    StructField("kline_start", StringType(),  True),
    StructField("kline_end",   StringType(),  True),
    StructField("open",        DoubleType(),  True),
    StructField("high",        DoubleType(),  True),
    StructField("low",         DoubleType(),  True),
    StructField("close",       DoubleType(),  True),
    StructField("volume",      DoubleType(),  True),
    StructField("trade_count", IntegerType(), True),
    StructField("is_closed",   BooleanType(), True),
])


# ─── Inisialisasi SparkSession ────────────────────────────────
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("btc-stream-processor")
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Kafka ack
        .config("spark.streaming.stopGracefullyOnShutdown",   "true")
        # Timezone UTC supaya timestamp konsisten
        .config("spark.sql.session.timeZone",                 "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─── Baca stream dari Kafka ───────────────────────────────────
def read_kafka_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe",               KAFKA_TOPIC_RAW)
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        # Batas batch supaya tidak overwhelm saat pertama start
        .option("maxOffsetsPerTrigger",    "1000")
        .load()
    )


# ─── Parse JSON dan filter kline closed ──────────────────────
def parse_and_filter(raw_df):
    parsed = (
        raw_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                KLINE_SCHEMA,
            ).alias("data")
        )
        .select("data.*")
    )

    # Hanya proses kline yang sudah closed (candle selesai)
    klines = (
        parsed
        .filter(F.col("event_type") == "kline")
        .filter(F.col("is_closed")  == True)     # noqa: E712
    )

    # Cast timestamp string → TimestampType
    klines = (
        klines
        .withColumn("window_start", F.to_timestamp("kline_start"))
        .withColumn("window_end",   F.to_timestamp("kline_end"))
        .drop("kline_start", "kline_end", "event_time", "event_type",
              "symbol", "interval", "is_closed")
    )

    return klines


# ─── Hitung rolling volatility (std return 5 candle) ─────────
def add_rolling_volatility(df):
    """
    Volatility = standar deviasi log-return dari 5 candle terakhir.
    Di Spark Structured Streaming, window function atas data historis
    tidak bisa langsung dipakai pada streaming DF.

    Strategi: hitung log-return per baris (close saat ini vs close
    sebelumnya) menggunakan lag — ini dibatasi oleh micro-batch.
    Untuk rolling 5-candle yang proper, kita simpan dulu ke PostgreSQL
    dan query ulang (foreachBatch pattern).

    Pada implementasi ini kita pakai foreachBatch sehingga setiap
    micro-batch bisa query baris sebelumnya dari PostgreSQL.
    """
    # Ditangani di fungsi write_batch di bawah
    return df


# ─── ForeachBatch: hitung volatility + tulis ke PostgreSQL ───
def write_batch(batch_df, batch_id: int):
    """
    Dipanggil untuk setiap micro-batch.
    1. Cek apakah batch kosong
    2. Query 5 candle terakhir dari PostgreSQL untuk hitung volatility
    3. Hitung rolling_volatility per baris baru
    4. Tulis ke btc_ohlc_1m
    """
    if batch_df.rdd.isEmpty():
        logger.info("Batch %d kosong, skip.", batch_id)
        return

    # Konversi ke Pandas untuk kemudahan join dengan data historis
    import pandas as pd
    import numpy as np
    import psycopg2

    rows = batch_df.toPandas()
    rows = rows.sort_values("window_start").reset_index(drop=True)

    # Query 5 close terakhir dari DB untuk baseline rolling volatility
    try:
        conn = psycopg2.connect(
            host=PG_JDBC_PROPERTIES.get("host",     os.getenv("APP_DB_HOST", "postgres")),
            port=int(os.getenv("APP_DB_PORT", "5432")),
            dbname=os.getenv("APP_DB_NAME",     "btcdb"),
            user=PG_JDBC_PROPERTIES["user"],
            password=PG_JDBC_PROPERTIES["password"],
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT close FROM btc_ohlc_1m
                ORDER BY window_start DESC
                LIMIT 5
                """
            )
            historical = [r[0] for r in cur.fetchall()]
        conn.close()
    except Exception as e:
        logger.warning("Gagal query historical close: %s", e)
        historical = []

    # Gabungkan historical + candle baru untuk hitung log-return
    all_closes = list(reversed(historical)) + list(rows["close"])

    def rolling_vol(closes, idx_in_batch):
        """Hitung std log-return dari 5 closes sebelum index ini."""
        global_idx = len(historical) + idx_in_batch
        window = all_closes[max(0, global_idx - 4): global_idx + 1]
        if len(window) < 2:
            return None
        # Cast ke float untuk handle decimal.Decimal dari PostgreSQL
        window = [float(x) for x in window]
        returns = np.diff(np.log(window))
        if len(returns) == 0:
            return None
        return float(np.std(returns))

    rows["rolling_volatility"] = [
        rolling_vol(all_closes, i) for i in range(len(rows))
    ]

    # Tulis ke PostgreSQL
    try:
        conn = psycopg2.connect(
            host=os.getenv("APP_DB_HOST", "postgres"),
            port=int(os.getenv("APP_DB_PORT", "5432")),
            dbname=os.getenv("APP_DB_NAME", "btcdb"),
            user=os.getenv("APP_DB_USER", "btcadmin"),
            password=os.getenv("APP_DB_PASSWORD", "gantiPasswordAman123"),
        )
        with conn.cursor() as cur:
            for _, row in rows.iterrows():
                cur.execute(
                    """
                    INSERT INTO btc_ohlc_1m
                        (window_start, window_end, open, high, low, close,
                         volume, trade_count, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        row["window_start"],
                        row["window_end"],
                        float(row["open"]),
                        float(row["high"]),
                        float(row["low"]),
                        float(row["close"]),
                        float(row["volume"]),
                        int(row["trade_count"]) if row["trade_count"] else 0,
                    ),
                )
        conn.commit()
        conn.close()
        logger.info(
            "Batch %d: berhasil tulis %d baris ke btc_ohlc_1m.",
            batch_id, len(rows),
        )
    except Exception as e:
        logger.error("Batch %d: gagal tulis ke PostgreSQL: %s", batch_id, e)
        raise


# ─── Main ─────────────────────────────────────────────────────
def main():
    logger.info("Memulai Spark Structured Streaming job...")

    spark = create_spark_session()
    raw_df = read_kafka_stream(spark)
    klines = parse_and_filter(raw_df)

    query = (
        klines
        .writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")   # proses setiap 30 detik
        .start()
    )

    logger.info("Streaming query started. Menunggu data dari Kafka...")
    query.awaitTermination()


if __name__ == "__main__":
    main()