import json
import logging
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import pybreaker
import requests
from prefect import flow, task
from prefect.client.schemas.schedules import IntervalSchedule
logger = logging.getLogger("sentiment_flow")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "k4ipbd_minio_2026")
PGBOUNCER_HOST   = os.getenv("PGBOUNCER_HOST", "pgbouncer")
PGBOUNCER_PORT   = int(os.getenv("PGBOUNCER_PORT", "6432"))
PG_DIRECT_HOST   = os.getenv("APP_DB_HOST", "postgres")
PG_DIRECT_PORT   = int(os.getenv("APP_DB_PORT", "5432"))
PG_DB            = os.getenv("APP_DB_NAME", "btcdb")
PG_USER          = os.getenv("APP_DB_USER", "kelompok4_ipbd")
PG_PASSWORD      = os.getenv("APP_DB_PASSWORD", "")
TWITTER_TOKEN    = os.getenv("TWITTER_AUTH_TOKEN", "")
SCRAPE_LIMIT     = int(os.getenv("TWITTER_SCRAPE_LIMIT", "200"))
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT    = os.getenv("TELEGRAM_CHAT_ID", "")
TWITTER_BUCKET   = "twitter-raw"
KEYWORDS         = ["bitcoin", "BTC", "crypto"]


def _pg_conn(host: str, port: int):
    return psycopg2.connect(
        host=host, port=port, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )


def _send_telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": msg},
            timeout=5,
        )
    except Exception:
        pass


def _on_open(cb):
    msg = f"[sentiment_flow] Circuit breaker OPEN: PostgreSQL gagal {cb.fail_counter}x"
    logger.error(msg)
    _send_telegram(msg)


pg_breaker = pybreaker.CircuitBreaker(fail_max=5, reset_timeout=60)
pg_breaker.add_listeners(type(
    "_L", (pybreaker.CircuitBreakerListener,),
    {"state_change": staticmethod(lambda cb, o, n: _on_open(cb) if str(n) == "open" else None)},
)())


def _minio_s3fs_path(object_path: str) -> str:
    ep = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    return f"s3://{TWITTER_BUCKET}/{object_path}"


def _s3fs_client():
    import s3fs
    ep = MINIO_ENDPOINT
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        endpoint_url=ep,
    )


@task(retries=3, retry_delay_seconds=300)
def harvest_tweets(window_start: datetime, run_id: str) -> str:
    import pandas as pd

    path = f"tweets/{window_start:%Y/%m/%d/%H_%M}.parquet"
    all_rows = []

    for keyword in KEYWORDS:
        with tempfile.TemporaryDirectory() as tmp:
            cmd = [
                "npx", "--yes", "tweet-harvest",
                "--token",          TWITTER_TOKEN,
                "--search-keyword", keyword,
                "--limit",          str(SCRAPE_LIMIT),
                "--export-format",  "csv",
                "--tab",            "LATEST",
            ]
            try:
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=300, cwd=tmp
                )
                if result.returncode != 0:
                    logger.warning("tweet-harvest gagal untuk '%s': %s", keyword, result.stderr[:200])
                    continue

                csv_files = sorted(
                    Path(tmp, "tweets-data").glob("*.csv"),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True,
                )
                if not csv_files:
                    continue

                df = pd.read_csv(csv_files[0])
                if df.empty:
                    continue

                df["keyword"] = keyword
                all_rows.append(df)
            except Exception as e:
                logger.warning("Error harvest '%s': %s", keyword, e)

    if not all_rows:
        raise RuntimeError("Tidak ada tweet yang berhasil di-harvest.")

    combined = pd.concat(all_rows, ignore_index=True)
    combined = combined.drop_duplicates(subset=["id_str"]) if "id_str" in combined.columns else combined


    fs = _s3fs_client()
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_f:
        combined.to_parquet(tmp_f.name, index=False)
        tmp_path = tmp_f.name

    with fs.open(f"{TWITTER_BUCKET}/{path}", "wb") as f:
        f.write(Path(tmp_path).read_bytes())
    Path(tmp_path).unlink(missing_ok=True)


    try:
        conn = _pg_conn(PGBOUNCER_HOST, PGBOUNCER_PORT)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_lineage
                    (run_id, pipeline_name, source, target_table, rows_processed,
                     quality_status, started_at, finished_at, params)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                """,
                (
                    run_id,
                    "sentiment_pipeline",
                    ",".join(KEYWORDS),
                    "sentiment_30m",
                    len(combined),
                    "ok",
                    window_start,
                    json.dumps({"path": path, "keywords": KEYWORDS}),
                ),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Gagal log pipeline_lineage (task 1): %s", e)

    logger.info("Harvested %d tweets → %s", len(combined), path)
    return path


@task(retries=3, retry_delay_seconds=300)
def score_and_store(parquet_path: str, window_start: datetime, run_id: str):
    import numpy as np
    import pandas as pd

    fs = _s3fs_client()


    with fs.open(f"{TWITTER_BUCKET}/{parquet_path}", "rb") as f:
        df = pd.read_parquet(f)

    if df.empty:
        raise ValueError("Parquet file kosong.")


    text_col = next((c for c in ["full_text", "text", "tweet_text"] if c in df.columns), None)
    if text_col is None:
        raise ValueError("Kolom teks tidak ditemukan di Parquet.")

    try:
        from finvader import finvader as _fv
        def _score(text):
            try:
                return _fv(str(text), use_sentibignomics=True, use_henry=True, indicator="compound")
            except Exception:
                return 0.0
    except ImportError:
        import nltk
        nltk.download('vader_lexicon', quiet=True)
        from nltk.sentiment.vader import SentimentIntensityAnalyzer
        _sia = SentimentIntensityAnalyzer()
        def _score(text):
            return _sia.polarity_scores(str(text))["compound"]

    df["compound"] = df[text_col].fillna("").apply(_score)


    errors = []
    required_cols = [text_col, "compound"]
    for col in required_cols:
        if df[col].isnull().any():
            errors.append(f"Null values in column '{col}'")
    if not (df["compound"].between(-1, 1).all()):
        errors.append("compound_score out of [-1, 1] range")
    if errors:
        raise ValueError(f"GE validation failed: {errors}")

    tweet_count = len(df)
    data_quality = "low_sample" if tweet_count < 5 else "ok"


    compound_mean = float(df["compound"].mean())
    pos_ratio     = float((df["compound"] > 0.05).sum() / tweet_count)
    neg_ratio     = float((df["compound"] < -0.05).sum() / tweet_count)
    neu_ratio     = float(1 - pos_ratio - neg_ratio)

    like_col = next((c for c in ["favorite_count", "like_count", "likes"] if c in df.columns), None)
    if like_col:
        weights = pd.to_numeric(df[like_col], errors="coerce").fillna(1).clip(lower=1)
    else:
        weights = pd.Series(np.ones(len(df)))
    weighted_compound = float((df["compound"] * weights).sum() / weights.sum())

    window_end = window_start + timedelta(minutes=30)


    @pg_breaker
    def _write():
        conn = _pg_conn(PGBOUNCER_HOST, PGBOUNCER_PORT)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sentiment_30m
                    (window_start, window_end, compound_score, positive_ratio,
                     negative_ratio, neutral_ratio, weighted_compound,
                     tweet_count, data_quality, source_file)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (window_start) DO UPDATE SET
                    compound_score    = EXCLUDED.compound_score,
                    positive_ratio    = EXCLUDED.positive_ratio,
                    negative_ratio    = EXCLUDED.negative_ratio,
                    neutral_ratio     = EXCLUDED.neutral_ratio,
                    weighted_compound = EXCLUDED.weighted_compound,
                    tweet_count       = EXCLUDED.tweet_count,
                    data_quality      = EXCLUDED.data_quality,
                    source_file       = EXCLUDED.source_file
                """,
                (
                    window_start, window_end,
                    round(compound_mean, 4), round(pos_ratio, 4),
                    round(neg_ratio, 4), round(max(0, neu_ratio), 4),
                    round(weighted_compound, 4),
                    tweet_count, data_quality, parquet_path,
                ),
            )
        conn.commit()
        conn.close()

    try:
        _write()
    except pybreaker.CircuitBreakerError:
        raise RuntimeError("Circuit breaker open — PostgreSQL tidak tersedia")

    logger.info(
        "sentiment_30m updated: window=%s, tweets=%d, compound=%.4f, quality=%s",
        window_start, tweet_count, compound_mean, data_quality,
    )


def _handle_failure(run_id: str, window_start: datetime, error: str):
    window_end = window_start + timedelta(minutes=30)
    try:
        conn = _pg_conn(PG_DIRECT_HOST, PG_DIRECT_PORT)
        with conn.cursor() as cur:

            cur.execute(
                "UPDATE pipeline_lineage SET quality_status = 'failed', finished_at = NOW() "
                "WHERE run_id = %s::uuid",
                (run_id,),
            )

            cur.execute(
                """
                SELECT compound_score, positive_ratio, negative_ratio,
                       neutral_ratio, weighted_compound, tweet_count
                FROM sentiment_30m
                WHERE data_quality != 'stale'
                ORDER BY window_start DESC LIMIT 1
                """
            )
            last = cur.fetchone()
            if last:
                cur.execute(
                    """
                    INSERT INTO sentiment_30m
                        (window_start, window_end, compound_score, positive_ratio,
                         negative_ratio, neutral_ratio, weighted_compound,
                         tweet_count, data_quality, source_file)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'stale', NULL)
                    ON CONFLICT (window_start) DO UPDATE SET data_quality = 'stale'
                    """,
                    (window_start, window_end, *last),
                )
        conn.commit()
        conn.close()
        logger.warning("Forward-fill stale row inserted for window %s", window_start)
    except Exception as e:
        logger.error("Gagal handle failure: %s", e)

    _send_telegram(
        f"[sentiment_pipeline] FAILED untuk window {window_start}: {error[:200]}"
    )


@flow(name="sentiment_pipeline", log_prints=True)
def sentiment_pipeline():
    import uuid
    now = datetime.now(tz=timezone.utc)
    window_start = now.replace(
        minute=(now.minute // 30) * 30, second=0, microsecond=0
    )
    run_id = str(uuid.uuid4())

    try:
        parquet_path = harvest_tweets(window_start, run_id)
        score_and_store(parquet_path, window_start, run_id)
    except Exception as e:
        _handle_failure(run_id, window_start, str(e))
        raise


if __name__ == "__main__":
    sentiment_pipeline()
