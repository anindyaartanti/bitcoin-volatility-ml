"""
prefect/flows/model_performance_check.py
==========================================
Prefect flow: model-performance-check
- Compute production RMSE/MAE (6-hour sliding window)
- Compare actual volatility vs predicted
- Alert if degradation > 50% from training baseline
- Schedule: setiap 1 jam
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta

import numpy as np
import psycopg2
import requests
from prefect import flow, task
from prefect.client.schemas.schedules import IntervalSchedule

logger = logging.getLogger("model_performance_check")

PGBOUNCER_HOST = os.getenv("PGBOUNCER_HOST", "pgbouncer")
PGBOUNCER_PORT = int(os.getenv("PGBOUNCER_PORT", "6432"))
PG_DIRECT_HOST = os.getenv("APP_DB_HOST", "postgres")
PG_DIRECT_PORT = int(os.getenv("APP_DB_PORT", "5432"))
PG_DB = os.getenv("APP_DB_NAME", "btcdb")
PG_USER = os.getenv("APP_DB_USER", "kelompok4_ipbd")
PG_PASSWORD = os.getenv("APP_DB_PASSWORD", "")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

TRAINING_BASELINE_MAE = 0.0015
DEGRADATION_THRESHOLD = 2.0


def _pg_conn():
    return psycopg2.connect(
        host=PGBOUNCER_HOST, port=PGBOUNCER_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
    )


def _send_telegram(msg: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": "HTML"},
            timeout=5,
        )
    except Exception:
        pass


@task(retries=2, retry_delay_seconds=30)
def compute_actual_volatility() -> int:
    """Backfill actual_vol in btc_predictions: match predicted_vol_5m to actual future volatility."""
    conn = _pg_conn()
    rows_updated = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH ohlc_returns AS (
                    SELECT
                        window_start,
                        (close - LAG(close) OVER (ORDER BY window_start))
                            / NULLIF(LAG(close) OVER (ORDER BY window_start), 0) AS ret
                    FROM btc_ohlc_1m
                ),
                future_vol AS (
                    SELECT
                        p.window_start AS ws,
                        STDDEV(r.ret) AS actual_vol_5m
                    FROM volatility_pred p
                    JOIN ohlc_returns r ON r.window_start > p.window_start
                                       AND r.window_start <= p.window_start + INTERVAL '5 minutes'
                    GROUP BY p.window_start
                )
                INSERT INTO btc_predictions (window_start, predicted_vol, actual_vol, model_version)
                SELECT
                    p.window_start,
                    p.predicted_vol_5m,
                    f.actual_vol_5m,
                    p.model_version
                FROM volatility_pred p
                LEFT JOIN future_vol f ON f.ws = p.window_start
                WHERE p.window_start > NOW() - INTERVAL '7 days'
                  AND p.window_start < NOW() - INTERVAL '5 minutes'
                  AND f.actual_vol_5m IS NOT NULL
                ON CONFLICT (window_start) DO UPDATE SET
                    actual_vol = EXCLUDED.actual_vol,
                    model_mae  = ABS(EXCLUDED.actual_vol - btc_predictions.predicted_vol)
                """
            )
            rows_updated = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    logger.info("Backfilled actual_vol: %d rows updated.", rows_updated)
    return rows_updated


@task(retries=2, retry_delay_seconds=30)
def compute_production_metrics() -> dict:
    """Sliding window RMSE/MAE over last 6 hours."""
    conn = _pg_conn()
    try:
        import pandas as pd
        df = pd.read_sql(
            """
            SELECT window_start, predicted_vol, actual_vol, model_version
            FROM btc_predictions
            WHERE window_start > NOW() - INTERVAL '12 hours'
              AND actual_vol IS NOT NULL
            ORDER BY window_start
            """,
            conn,
        )
    finally:
        conn.close()

    if df.empty or len(df) < 10:
        return {"rmse": None, "mae": None, "n": len(df), "model_version": "none"}

    errors = df["predicted_vol"].values - df["actual_vol"].values
    rmse = float(np.sqrt(np.mean(errors ** 2)))
    mae = float(np.mean(np.abs(errors)))
    model_version = str(df["model_version"].iloc[-1])

    logger.info("Production metrics (n=%d): RMSE=%.6f, MAE=%.6f", len(df), rmse, mae)
    return {"rmse": rmse, "mae": mae, "n": len(df), "model_version": model_version}


@task(retries=2, retry_delay_seconds=30)
def store_and_alert(metrics: dict) -> None:
    if metrics["rmse"] is None:
        logger.info("Not enough data for production metrics, skipping.")
        return

    now = datetime.now(timezone.utc)
    window_end = now
    window_start = now - timedelta(hours=6)

    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO model_performance
                    (model_version, window_start, window_end, rmse, mae, n_predictions)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (metrics["model_version"], window_start, window_end,
                 metrics["rmse"], metrics["mae"], metrics["n"]),
            )
        conn.commit()
    finally:
        conn.close()

    degradation = metrics["mae"] / TRAINING_BASELINE_MAE
    if degradation > DEGRADATION_THRESHOLD:
        _send_telegram(
            f"<b>[ML PERFORMANCE] Model degradation detected!</b>\n"
            f"Version: {metrics['model_version']}\n"
            f"Prod MAE: {metrics['mae']:.6f} vs Baseline: {TRAINING_BASELINE_MAE:.6f}\n"
            f"Degradation: {degradation:.1f}x\n"
            f"N predictions: {metrics['n']}"
        )


@flow(name="model-performance-check", log_prints=True)
def model_performance_check() -> None:
    compute_actual_volatility()
    metrics = compute_production_metrics()
    store_and_alert(metrics)


def deploy() -> None:
    from prefect.deployments import Deployment
    from prefect.client.schemas.objects import MinimalDeploymentSchedule

    Deployment.build_from_flow(
        flow=model_performance_check,
        name="model-performance-check-hourly",
        work_pool_name="default",
        schedules=[MinimalDeploymentSchedule(
            schedule=IntervalSchedule(interval=3600)
        )],
        apply=True,
    )
    print("Deployment 'model-performance-check-hourly' created.")
