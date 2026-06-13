"""
prefect/flows/training_flow.py
================================
Prefect flow: model-training
- Buat/replace PostgreSQL view v_ml_features (JOIN OHLC + sentimen, forward-fill)
- Extract features dari view
- Train XGBoost dengan TimeSeriesSplit 5-fold + StandardScaler
- Log ke MLflow, promote ke Production jika MAE < threshold
- Record lineage ke pipeline_lineage via PgBouncer
- Schedule: setiap Senin 02:00 UTC
"""

import json
import logging
import os
import tempfile
from datetime import datetime, timezone

import joblib
import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import psycopg2
import requests
from mlflow.tracking import MlflowClient
from prefect import flow, task
from prefect.deployments import Deployment
from prefect.client.schemas.schedules import CronSchedule
from prefect.client.schemas.objects import MinimalDeploymentSchedule
from prefect.logging import get_run_logger
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine, text
from xgboost import XGBRegressor

logger = logging.getLogger("training_flow")

# ─── Env ─────────────────────────────────────────────────────
_PG_HOST = os.getenv("APP_DB_HOST", "postgres")
_PG_PORT = os.getenv("APP_DB_PORT", "5432")
_PG_DB   = os.getenv("APP_DB_NAME", "btcdb")
_PG_USER = os.getenv("APP_DB_USER", "btcadmin")
_PG_PASS = os.getenv("APP_DB_PASSWORD", "")

_PGB_HOST = os.getenv("PGBOUNCER_HOST", "pgbouncer")
_PGB_PORT = os.getenv("PGBOUNCER_PORT", "6432")

WRITE_DB_URL    = f"postgresql+psycopg2://{_PG_USER}:{_PG_PASS}@{_PG_HOST}:{_PG_PORT}/{_PG_DB}"
PGBOUNCER_DSN   = dict(host=_PGB_HOST, port=int(_PGB_PORT), dbname=_PG_DB,
                       user=_PG_USER, password=_PG_PASS)

MLFLOW_URI      = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT   = os.getenv("TELEGRAM_CHAT_ID", "")

MAE_THRESHOLD = 0.0015
MODEL_NAME    = "btc_volatility_xgb"
FEATURE_COLS  = [
    "rolling_vol_5m", "price_range_ratio", "vol_ratio",
    "compound_score", "positive_ratio", "tweet_count", "minutes_since_sentiment",
]
TARGET_COL = "target_vol_5m"

XGB_PARAMS = dict(
    n_estimators=300,
    max_depth=4,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    min_child_weight=5,
    objective="reg:squarederror",
    eval_metric="mae",
    random_state=42,
    early_stopping_rounds=20,
    tree_method="hist",
)

# ─── View SQL ────────────────────────────────────────────────
_VIEW_SQL = """
CREATE OR REPLACE VIEW v_ml_features AS
WITH
-- Batas waktu dari btc_ohlc_1m untuk generate_series sentimen
bounds AS (
    SELECT MIN(window_start) AS t_min, MAX(window_start) AS t_max
    FROM btc_ohlc_1m
),
-- Fitur OHLC: rolling volatility, price range, volume ratio, target forward vol
ohlc_calc AS (
    SELECT
        window_start,
        close,
        -- Std dev log-return 5 menit (rolling)
        STDDEV(
            (close - LAG(close) OVER w) / NULLIF(LAG(close) OVER w, 0)
        ) OVER (
            ORDER BY window_start
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS rolling_vol_5m,
        -- Price range relatif terhadap close
        (high - low) / NULLIF(close, 0) AS price_range_ratio,
        -- Volume relatif terhadap rata-rata 10 menit
        volume / NULLIF(
            AVG(volume) OVER (ORDER BY window_start ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),
        0) AS vol_ratio,
        -- Target: std dev log-return 5 menit ke depan
        STDDEV(
            (close - LAG(close) OVER w) / NULLIF(LAG(close) OVER w, 0)
        ) OVER (
            ORDER BY window_start
            ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING
        ) AS target_vol_5m
    FROM btc_ohlc_1m
    WINDOW w AS (ORDER BY window_start)
),
-- Expand sentimen 30 menit → per menit menggunakan generate_series
sentiment_series AS (
    SELECT
        gs.t AS window_start,
        s.compound_score,
        s.positive_ratio,
        s.tweet_count,
        s.window_start AS sentiment_window_start
    FROM bounds,
         generate_series(bounds.t_min, bounds.t_max, INTERVAL '1 minute') AS gs(t)
    LEFT JOIN sentiment_30m s
           ON s.window_start <= gs.t
          AND s.window_end   >  gs.t
),
-- Forward-fill compound_score, positive_ratio, tweet_count
-- PostgreSQL tidak support IGNORE NULLS → pakai MAX(CASE WHEN) OVER window
sentiment_ffill AS (
    SELECT
        window_start,
        -- Forward-fill: ambil nilai non-null terakhir
        MAX(CASE WHEN compound_score  IS NOT NULL THEN compound_score  END)
            OVER (ORDER BY window_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS compound_score,
        MAX(CASE WHEN positive_ratio  IS NOT NULL THEN positive_ratio  END)
            OVER (ORDER BY window_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS positive_ratio,
        MAX(CASE WHEN tweet_count     IS NOT NULL THEN tweet_count     END)
            OVER (ORDER BY window_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS tweet_count,
        -- Timestamp sentimen terakhir yang valid (untuk hitung minutes_since_sentiment)
        MAX(CASE WHEN sentiment_window_start IS NOT NULL THEN sentiment_window_start END)
            OVER (ORDER BY window_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS last_sentiment_ts
    FROM sentiment_series
)
SELECT
    o.window_start,
    o.rolling_vol_5m,
    o.price_range_ratio,
    o.vol_ratio,
    sf.compound_score,
    sf.positive_ratio,
    sf.tweet_count::FLOAT  AS tweet_count,
    -- Berapa menit sejak sentimen terakhir diperbarui
    EXTRACT(EPOCH FROM (o.window_start - sf.last_sentiment_ts)) / 60.0
        AS minutes_since_sentiment,
    o.target_vol_5m
FROM ohlc_calc o
JOIN sentiment_ffill sf ON sf.window_start = o.window_start
WHERE o.target_vol_5m   IS NOT NULL
  AND o.rolling_vol_5m  IS NOT NULL
  AND sf.compound_score IS NOT NULL
ORDER BY o.window_start;
"""


def ensure_view_exists(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_VIEW_SQL))


def _send_telegram(msg: str) -> None:
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


# ─── Task 1: Extract features ─────────────────────────────────
@task(retries=2, retry_delay_seconds=30)
def extract_features() -> pd.DataFrame:
    log = get_run_logger()
    engine = create_engine(WRITE_DB_URL)
    ensure_view_exists(engine)
    df = pd.read_sql("SELECT * FROM v_ml_features ORDER BY window_start", engine)
    engine.dispose()
    log.info("v_ml_features: %d baris, %d kolom", len(df), df.shape[1])
    if len(df) < 500:
        raise ValueError(
            f"Data tidak cukup: hanya {len(df)} baris di v_ml_features "
            f"(minimum 500). Tunggu lebih banyak data OHLC dan sentimen."
        )
    return df


# ─── Task 2: Train model ──────────────────────────────────────
@task(retries=2, retry_delay_seconds=30)
def train_model(df: pd.DataFrame):
    log = get_run_logger()
    X = df[FEATURE_COLS].fillna(0).values
    y = df[TARGET_COL].fillna(0).values

    tscv = TimeSeriesSplit(n_splits=5)
    mae_scores, rmse_scores = [], []

    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        X_train, X_val = X[train_idx], X[val_idx]
        y_train, y_val = y[train_idx], y[val_idx]

        scaler = StandardScaler()
        X_train_s = scaler.fit_transform(X_train)
        X_val_s   = scaler.transform(X_val)

        model = XGBRegressor(**XGB_PARAMS)
        model.fit(
            X_train_s, y_train,
            eval_set=[(X_val_s, y_val)],
            verbose=False,
        )
        y_pred = model.predict(X_val_s)
        mae_scores.append(float(mean_absolute_error(y_val, y_pred)))
        rmse_scores.append(float(np.sqrt(mean_squared_error(y_val, y_pred))))
        log.info("Fold %d — MAE=%.6f RMSE=%.6f", fold + 1, mae_scores[-1], rmse_scores[-1])

    # Final model: fit pada seluruh dataset
    final_scaler = StandardScaler()
    X_all = final_scaler.fit_transform(X)
    final_model = XGBRegressor(**XGB_PARAMS)
    # Early stopping butuh eval_set — pakai 10% terakhir sebagai val internal
    split = int(len(X_all) * 0.9)
    final_model.fit(
        X_all[:split], y[:split],
        eval_set=[(X_all[split:], y[split:])],
        verbose=False,
    )

    metrics = {
        "mae_cv_mean": float(np.mean(mae_scores)),
        "mae_cv_std":  float(np.std(mae_scores)),
        "rmse_cv_mean": float(np.mean(rmse_scores)),
        "n_rows": len(df),
    }
    log.info(
        "CV selesai — MAE mean=%.6f std=%.6f | RMSE mean=%.6f",
        metrics["mae_cv_mean"], metrics["mae_cv_std"], metrics["rmse_cv_mean"],
    )
    return final_model, final_scaler, metrics


# ─── Task 3: Log ke MLflow ────────────────────────────────────
@task(retries=2, retry_delay_seconds=30)
def log_to_mlflow(model_tuple) -> dict:
    log = get_run_logger()
    final_model, final_scaler, metrics = model_tuple

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("btc_volatility_prediction")

    with mlflow.start_run(run_name="xgboost_timeseries_cv") as run:
        mlflow.log_params({k: v for k, v in XGB_PARAMS.items()})
        mlflow.log_metrics(metrics)

        fi = {feat: float(imp) for feat, imp in
              zip(FEATURE_COLS, final_model.feature_importances_)}
        mlflow.log_dict(fi, "feature_importance.json")

        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
            joblib.dump(final_scaler, f.name)
            mlflow.log_artifact(f.name, artifact_path="scaler")

        mlflow.xgboost.log_model(
            final_model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
        )
        run_id = run.info.run_id

    mae = metrics["mae_cv_mean"]
    promoted = False

    if mae < MAE_THRESHOLD:
        client = MlflowClient(tracking_uri=MLFLOW_URI)
        latest = client.get_latest_versions(MODEL_NAME, stages=["None"])
        if latest:
            client.transition_model_version_stage(
                name=MODEL_NAME,
                version=latest[0].version,
                stage="Production",
                archive_existing_versions=True,
            )
            promoted = True
            log.info("Model v%s dipromosikan ke Production (MAE=%.6f)", latest[0].version, mae)
    else:
        msg = (
            f"⚠️ Model training selesai tapi MAE={mae:.6f} melebihi "
            f"threshold {MAE_THRESHOLD}. Model tidak dipromote ke Production."
        )
        log.warning(msg)
        _send_telegram(msg)

    return {"run_id": run_id, "promoted": promoted, "mae": mae}


# ─── Task 4: Record lineage ───────────────────────────────────
@task(retries=2, retry_delay_seconds=30)
def record_lineage(metrics: dict, mlflow_result: dict, n_rows: int) -> None:
    log = get_run_logger()
    quality_status = "promoted" if mlflow_result["promoted"] else "skipped"
    params_json = json.dumps({
        "run_id":       mlflow_result["run_id"],
        "mae_cv_mean":  metrics["mae_cv_mean"],
        "promoted":     mlflow_result["promoted"],
        "model_name":   MODEL_NAME,
    })
    try:
        conn = psycopg2.connect(**PGBOUNCER_DSN)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_lineage
                    (pipeline_name, source, target_table, rows_processed,
                     rows_rejected, quality_status, started_at, finished_at, params)
                VALUES (%s, %s, %s, %s, %s, %s,
                        NOW() - INTERVAL '10 minutes', NOW(), %s)
                """,
                (
                    "model_training",
                    "v_ml_features",
                    "mlflow_registry",
                    n_rows,
                    0,
                    quality_status,
                    params_json,
                ),
            )
        conn.commit()
        conn.close()
        log.info("Lineage dicatat: status=%s rows=%d", quality_status, n_rows)
    except Exception as e:
        log.warning("Gagal catat pipeline_lineage: %s", e)


# ─── Main Flow ────────────────────────────────────────────────
@flow(name="model-training", log_prints=True)
def training_flow() -> None:
    df = extract_features()
    model_tuple = train_model(df)
    mlflow_result = log_to_mlflow(model_tuple)
    record_lineage(model_tuple[2], mlflow_result, len(df))


# ─── Deployment helpers ───────────────────────────────────────
def deploy() -> None:
    """Dipanggil oleh deploy_all.py untuk mendaftarkan deployment ke Prefect."""
    Deployment.build_from_flow(
        flow=training_flow,
        name="model-training-weekly",
        work_pool_name="default",
        schedules=[MinimalDeploymentSchedule(
            schedule=CronSchedule(cron="0 2 * * 1")
        )],
        apply=True,
    )
    print("Deployment 'model-training-weekly' created.")


if __name__ == "__main__":
    training_flow.serve(name="model-training-weekly", cron="0 2 * * 1")
