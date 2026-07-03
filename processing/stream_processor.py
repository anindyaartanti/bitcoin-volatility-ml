import json
import logging
import os
import pickle
import sys
import threading
import time
from datetime import datetime, timezone

import joblib
import numpy as np
import pybreaker
import requests
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
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
    CHECKPOINT_PATH,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("stream_processor")

PGBOUNCER_HOST  = os.getenv("PGBOUNCER_HOST", "pgbouncer")
PGBOUNCER_PORT  = int(os.getenv("PGBOUNCER_PORT", "6432"))
PG_DIRECT_HOST  = os.getenv("APP_DB_HOST", "postgres")
PG_DIRECT_PORT  = int(os.getenv("APP_DB_PORT", "5432"))
PG_DB           = os.getenv("APP_DB_NAME", "btcdb")
PG_USER         = os.getenv("APP_DB_USER", "kelompok4_ipbd")
PG_PASSWORD     = os.getenv("APP_DB_PASSWORD", "")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "btc_ticker_dlq")
MLFLOW_URI      = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
XGB_CACHE_PATH  = "/tmp/xgb_model_cache"
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT   = os.getenv("TELEGRAM_CHAT_ID", "")

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "")


@F.udf(StringType())
def decrypt_udf(encrypted_bytes):
    if encrypted_bytes is None:
        return None
    _key = os.environ.get("ENCRYPTION_KEY", "")
    if not _key:
        return encrypted_bytes.decode("utf-8", errors="replace")
    try:
        from cryptography.fernet import Fernet as _Fernet
        return _Fernet(_key.encode()).decrypt(
            bytes(encrypted_bytes)
        ).decode("utf-8")
    except Exception:
        return encrypted_bytes.decode("utf-8", errors="replace")


FEATURE_COLS = [
    "rolling_vol_5m", "price_range_ratio", "vol_ratio",
    "compound_score", "positive_ratio", "tweet_count", "minutes_since_sentiment",
]

_sentiment_cache = {
    "compound_score": 0.0,
    "positive_ratio": 0.5,
    "tweet_count": 0.0,
    "minutes_since_sentiment": 30.0,
    "updated_at": None,
}

from collections import deque
_OHLC_BUFFER_SIZE = 30
_ohlc_buffer = deque(maxlen=_OHLC_BUFFER_SIZE)


def _append_to_buffer(rows: "pd.DataFrame") -> None:
    cols = ["window_start", "open", "high", "low", "close", "volume", "trade_count", "volatility"]
    for _, row in rows.iterrows():
        _ohlc_buffer.append({
            "window_start": row["window_start"],
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"]),
            "trade_count": int(row["trade_count"]),
            "volatility": float(row["volatility"]) if row["volatility"] is not None else None,
        })

TRADE_SCHEMA = StructType([
    StructField("event_time", LongType(),   True),
    StructField("symbol",     StringType(), True),
    StructField("trade_id",   LongType(),   True),
    StructField("price",      StringType(), True),
    StructField("quantity",   StringType(), True),
    StructField("is_buyer_mm", BooleanType(), True),
])

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


_INFERENCE_ARTIFACTS = None

def _load_model_version(client) -> dict:
    try:
        mv = client.get_model_version_by_alias("btc_volatility_xgb", "production")
        return {"version": mv.version, "run_id": mv.run_id, "source": "alias"}
    except Exception:
        pass
    try:
        versions = client.search_model_versions("name='btc_volatility_xgb'")
        if versions:
            latest = max(versions, key=lambda v: int(v.version))
            return {"version": latest.version, "run_id": latest.run_id, "source": "search"}
    except Exception:
        pass
    return None


def load_model_and_scaler():
    global _INFERENCE_ARTIFACTS
    import pickle, json, joblib
    from mlflow.tracking import MlflowClient
    import mlflow.xgboost

    cache_dir = XGB_CACHE_PATH
    model_cache = f"{cache_dir}/model.pkl"
    scaler_cache = f"{cache_dir}/scaler.pkl"
    meta_cache = f"{cache_dir}/meta.json"

    try:
        mlflow.set_tracking_uri(MLFLOW_URI)
        client = MlflowClient()
        info = _load_model_version(client)
        if info is None:
            logger.warning("Model btc_volatility_xgb belum terdaftar di MLflow.")
        else:
            run_id = info["run_id"]
            version = info["version"]
            model = mlflow.xgboost.load_model(f"runs:/{run_id}/model")

            os.makedirs(cache_dir, exist_ok=True)
            scaler_artifact_dir = os.path.join(cache_dir, "scaler")
            local_dir = client.download_artifacts(run_id, "scaler", dst_path=cache_dir)
            scaler_path = os.path.join(local_dir, "scaler.pkl")
            if not os.path.exists(scaler_path):
                scaler_path = os.path.join(cache_dir, "scaler", "scaler.pkl")
            scaler = joblib.load(scaler_path)

            with open(model_cache, "wb") as f:
                pickle.dump(model, f)
            with open(scaler_cache, "wb") as f:
                pickle.dump(scaler, f)
            with open(meta_cache, "w") as f:
                json.dump({"model_version": version, "run_id": run_id}, f)

            logger.info("Model+scaler loaded from MLflow (v%s, source=%s)", version, info["source"])
            _INFERENCE_ARTIFACTS = (model, scaler, version)
            return True
    except Exception as e:
        logger.warning("MLflow load failed (%s: %s), fallback ke cache...", type(e).__name__, e)

    if os.path.exists(model_cache) and os.path.exists(scaler_cache):
        with open(model_cache, "rb") as f:
            model = pickle.load(f)
        with open(scaler_cache, "rb") as f:
            scaler = pickle.load(f)
        meta = {}
        if os.path.exists(meta_cache):
            with open(meta_cache) as f:
                meta = json.load(f)
        logger.info("Model+scaler loaded from cache (v%s)", meta.get("model_version", "?"))
        _INFERENCE_ARTIFACTS = (model, scaler, meta.get("model_version", "0"))
        return True

    logger.warning("Model+scaler tidak tersedia — inference akan di-skip")
    _INFERENCE_ARTIFACTS = (None, None, None)
    return False


def start_model_refresh_thread():
    def _loop():
        while True:
            logger.info("Periodic reload: mencoba load model dari MLflow...")
            ok = load_model_and_scaler()
            if ok:
                _, _, version = _INFERENCE_ARTIFACTS or (None, None, None)
                logger.info("Periodic reload: model v%s loaded", version)
            time.sleep(300)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


def refresh_sentiment_cache():
    try:
        conn = _pg_conn(PG_DIRECT_HOST, PG_DIRECT_PORT)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT compound_score, positive_ratio, tweet_count,
                       EXTRACT(EPOCH FROM (NOW() - window_start)) / 60.0
                           AS minutes_since_sentiment
                FROM sentiment_30m
                ORDER BY window_start DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if row:
                _sentiment_cache["compound_score"]          = float(row[0]) if row[0] is not None else 0.0
                _sentiment_cache["positive_ratio"]          = float(row[1]) if row[1] is not None else 0.5
                _sentiment_cache["tweet_count"]             = float(row[2]) if row[2] is not None else 0.0
                _sentiment_cache["minutes_since_sentiment"] = float(row[3]) if row[3] is not None else 30.0
                _sentiment_cache["updated_at"]              = datetime.now(timezone.utc)
                logger.info(
                    "Sentiment cache diperbarui: compound=%.4f, minutes_since=%.1f",
                    _sentiment_cache["compound_score"],
                    _sentiment_cache["minutes_since_sentiment"],
                )
        conn.close()
    except Exception as e:
        logger.warning("Gagal refresh sentiment cache: %s", e)


def start_sentiment_cache_thread():
    def _loop():
        while True:
            refresh_sentiment_cache()
            time.sleep(1800)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


def ensure_volatility_pred_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS volatility_pred (
                id                      BIGSERIAL PRIMARY KEY,
                window_start            TIMESTAMPTZ NOT NULL UNIQUE,
                predicted_vol_5m        NUMERIC(10,8) NOT NULL CHECK (predicted_vol_5m >= 0),
                rolling_vol_5m          NUMERIC(10,8),
                price_range_ratio       NUMERIC(8,6),
                vol_ratio               NUMERIC(8,4),
                compound_score          NUMERIC(6,4),
                minutes_since_sentiment NUMERIC(6,2),
                model_version           VARCHAR(20),
                inference_latency_ms    INTEGER,
                created_at              TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_vp_window ON volatility_pred (window_start DESC);
            """
        )
        try:
            cur.execute("GRANT SELECT ON volatility_pred TO dashboard_reader;")
        except Exception as e:
            logger.warning("Gagal grant ke dashboard_reader (role mungkin belum ada): %s", e)
    conn.commit()


def write_predictions(pred_rows, conn):
    with conn.cursor() as cur:
        for _, row in pred_rows.iterrows():
            cur.execute(
                """
                INSERT INTO volatility_pred
                    (window_start, predicted_vol_5m, rolling_vol_5m, price_range_ratio,
                     vol_ratio, compound_score, minutes_since_sentiment,
                     model_version, inference_latency_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (window_start) DO UPDATE SET
                    predicted_vol_5m     = EXCLUDED.predicted_vol_5m,
                    model_version        = EXCLUDED.model_version,
                    inference_latency_ms = EXCLUDED.inference_latency_ms
                """,
                (
                    row["window_start"],
                    float(row["predicted_vol_5m"]),
                    float(row["rolling_vol_5m"])      if row["rolling_vol_5m"] is not None      else None,
                    float(row["price_range_ratio"])   if row["price_range_ratio"] is not None   else None,
                    float(row["vol_ratio"])           if row["vol_ratio"] is not None           else None,
                    float(row["compound_score"]),
                    float(row["minutes_since_sentiment"]),
                    str(row["model_version"]),
                    int(row["inference_latency_ms"]),
                ),
            )
    conn.commit()

    try:
        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        for _, row in pred_rows.iterrows():
            msg = json.dumps({
                "window_start":     row["window_start"].isoformat() if hasattr(row["window_start"], "isoformat") else str(row["window_start"]),
                "predicted_vol_5m": float(row["predicted_vol_5m"]),
                "model_version":    str(row["model_version"]),
            })
            producer.produce("volatility_pred", msg.encode("utf-8"))
        producer.flush()
    except Exception as e:
        logger.warning("Gagal produce prediksi ke Kafka: %s", e)


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
            F.from_json(decrypt_udf(F.col("value")), TRADE_SCHEMA).alias("d")
        )
        .select(

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


def write_batch(batch_df, batch_id: int, xgb_model=None, scaler=None, model_version="cached"):
    if batch_df.rdd.isEmpty():
        logger.info("Batch %d kosong.", batch_id)
        return

    rows = batch_df.toPandas()
    rows_written = 0
    rows_rejected = 0
    inference_rows_count = 0
    infer_model_version = "none"

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


    _append_to_buffer(rows)

    if len(_ohlc_buffer) < 6:
        logger.info("Batch %d: buffer too small (%d < 6), skip inference.",
                     batch_id, len(_ohlc_buffer))
    else:
        import pandas as pd
        buf_df = pd.DataFrame(list(_ohlc_buffer))

        buf_df["rolling_vol_5m"]    = buf_df["close"].pct_change().rolling(5).std()
        buf_df["price_range_ratio"] = (buf_df["high"] - buf_df["low"]) / buf_df["close"].replace(0, float("nan"))
        buf_df["vol_ratio"]         = buf_df["volume"] / buf_df["volume"].rolling(10).mean()

        new_mask = buf_df["window_start"].isin(rows["window_start"].values)
        infer_rows = buf_df[new_mask & buf_df["rolling_vol_5m"].notna()].copy()

        inference_rows_count = len(infer_rows)

        if not infer_rows.empty and xgb_model is not None and scaler is not None:
            infer_rows["compound_score"]          = _sentiment_cache["compound_score"]
            infer_rows["positive_ratio"]          = _sentiment_cache["positive_ratio"]
            infer_rows["tweet_count"]             = _sentiment_cache["tweet_count"]
            infer_rows["minutes_since_sentiment"] = _sentiment_cache["minutes_since_sentiment"]

            feature_matrix = infer_rows[FEATURE_COLS].values
            t0 = time.time()
            scaled_features = scaler.transform(feature_matrix)
            predictions     = xgb_model.predict(scaled_features)
            inference_latency_ms = int((time.time() - t0) * 1000)

            infer_rows["predicted_vol_5m"]     = predictions
            infer_rows["model_version"]        = model_version
            infer_rows["inference_latency_ms"]  = inference_latency_ms
            infer_model_version  = model_version

            logger.info(
                "Batch %d: %d prediksi (latency=%dms, model=%s)",
                batch_id, inference_rows_count, inference_latency_ms, model_version,
            )

            pred_cols = [
                "window_start", "predicted_vol_5m", "rolling_vol_5m", "price_range_ratio",
                "vol_ratio", "compound_score", "minutes_since_sentiment",
                "model_version", "inference_latency_ms",
            ]
            try:
                pred_conn = _pg_conn(PGBOUNCER_HOST, PGBOUNCER_PORT)

                @pg_breaker
                def _write_preds(c):
                    write_predictions(infer_rows[pred_cols], c)

                _write_preds(pred_conn)
                pred_conn.close()
            except pybreaker.CircuitBreakerError:
                logger.warning("Batch %d: circuit breaker OPEN — skip write_predictions.", batch_id)
            except Exception as e:
                logger.warning("Batch %d: gagal write_predictions: %s", batch_id, e)


    try:
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
                    json.dumps({
                        "window":          "1 minute",
                        "batch_id":        batch_id,
                        "model_version":   infer_model_version,
                        "inference_rows":  inference_rows_count,
                    }),
                ),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Gagal log pipeline_lineage: %s", e)


def main():
    logger.info("Memulai Spark Structured Streaming job...")
    load_model_and_scaler()
    start_model_refresh_thread()

    start_sentiment_cache_thread()

    conn_direct = _pg_conn(PG_DIRECT_HOST, PG_DIRECT_PORT)
    ensure_volatility_pred_table(conn_direct)
    conn_direct.close()

    spark = create_spark_session()
    windowed = build_stream(spark)

    def _write_batch(batch_df, batch_id):
        model, scaler, model_version = _INFERENCE_ARTIFACTS
        write_batch(batch_df, batch_id, model, scaler, model_version)

    query = (
        windowed
        .writeStream
        .foreachBatch(_write_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Streaming query started. Menunggu data dari Kafka...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
