"""
airflow/dags/model_training.py
================================
DAG: Training XGBoost Regressor untuk prediksi volatilitas Bitcoin.
- Query fitur dari PostgreSQL (btc_ohlc_1m JOIN sentiment_hourly)
- Training dengan XGBoost
- Tracking eksperimen via MLflow
- Registrasi model terbaik ke MLflow Model Registry (stage: Production)

Schedule: mingguan setiap Senin pukul 02.00 (0 2 * * 1)
Bisa juga dijalankan manual dari Airflow UI.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# ─── Default args ────────────────────────────────────────────
default_args = {
    "owner": "bigdata-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "email_on_failure": False,
}


# ─── Task: cek apakah data cukup untuk training ──────────────
def check_training_data(**context):
    """
    Cek minimal 1000 baris OHLC dan 24 baris sentimen tersedia.
    Jika tidak cukup, skip training.
    """
    import psycopg2

    conn = psycopg2.connect(
        host=os.getenv("APP_DB_HOST", "postgres"),
        port=int(os.getenv("APP_DB_PORT", 5432)),
        dbname=os.getenv("APP_DB_NAME", "btcdb"),
        user=os.getenv("APP_DB_USER", "btcadmin"),
        password=os.getenv("APP_DB_PASSWORD", ""),
    )
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM btc_ohlc_1m")
        ohlc_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM sentiment_hourly")
        sentiment_count = cur.fetchone()[0]
    conn.close()

    print(f"Data tersedia: OHLC={ohlc_count}, Sentimen={sentiment_count}")

    if ohlc_count < 1000 or sentiment_count < 24:
        print("Data tidak cukup untuk training. Melewati model_training.")
        return "skip_training"

    return "prepare_features"


# ─── Task: siapkan fitur training ────────────────────────────
def prepare_features(**context):
    """
    Query dan gabungkan fitur dari PostgreSQL.
    Simpan sementara ke XCom (untuk dataset kecil) atau file.
    """
    import psycopg2
    import pandas as pd
    import json

    conn = psycopg2.connect(
        host=os.getenv("APP_DB_HOST", "postgres"),
        port=int(os.getenv("APP_DB_PORT", 5432)),
        dbname=os.getenv("APP_DB_NAME", "btcdb"),
        user=os.getenv("APP_DB_USER", "btcadmin"),
        password=os.getenv("APP_DB_PASSWORD", ""),
    )

    query = """
        WITH ohlc_features AS (
            SELECT
                window_start,
                open, high, low, close, volume, trade_count,
                -- Rolling volatility (std of returns, 5 candles)
                STDDEV(
                    LN(close / LAG(close) OVER (ORDER BY window_start))
                ) OVER (ORDER BY window_start ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
                    AS rolling_vol_5m,
                -- Price range ratio
                (high - low) / NULLIF(open, 0) AS price_range_ratio,
                -- Volume change
                volume / NULLIF(LAG(volume) OVER (ORDER BY window_start), 0) AS vol_ratio,
                -- Target: volatility 5 candles ahead (apa yang akan diprediksi)
                LEAD(
                    STDDEV(LN(close / LAG(close) OVER (ORDER BY window_start)))
                    OVER (ORDER BY window_start ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
                    5
                ) OVER (ORDER BY window_start) AS target_vol_5m
            FROM btc_ohlc_1m
        ),
        sentiment_features AS (
            SELECT
                DATE_TRUNC('hour', hour_timestamp) AS hour_ts,
                avg_sentiment,
                mention_count,
                positive_count::FLOAT / NULLIF(mention_count, 0) AS positive_ratio
            FROM sentiment_hourly
        )
        SELECT
            o.window_start,
            o.close, o.volume, o.trade_count,
            o.rolling_vol_5m, o.price_range_ratio, o.vol_ratio,
            COALESCE(s.avg_sentiment, 0)     AS avg_sentiment,
            COALESCE(s.mention_count, 0)     AS mention_count,
            COALESCE(s.positive_ratio, 0.5)  AS positive_ratio,
            o.target_vol_5m
        FROM ohlc_features o
        LEFT JOIN sentiment_features s
            ON DATE_TRUNC('hour', o.window_start) = s.hour_ts
        WHERE o.target_vol_5m IS NOT NULL
          AND o.rolling_vol_5m IS NOT NULL
        ORDER BY o.window_start
    """

    df = pd.read_sql(query, conn)
    conn.close()

    print(f"Dataset siap: {len(df)} baris, {df.shape[1]} kolom")

    # Simpan ke file sementara (lebih aman dari XCom untuk dataset besar)
    tmp_path = "/tmp/btc_training_features.parquet"
    df.to_parquet(tmp_path, index=False)
    context["ti"].xcom_push(key="feature_path", value=tmp_path)
    context["ti"].xcom_push(key="sample_count", value=len(df))


# ─── Task: training XGBoost ──────────────────────────────────
def train_xgboost(**context):
    """
    Training XGBRegressor, tracking MLflow, registrasi model Production.
    """
    import mlflow
    import mlflow.xgboost
    import pandas as pd
    import numpy as np
    from xgboost import XGBRegressor
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_squared_error, mean_absolute_error

    ti = context["ti"]
    feature_path = ti.xcom_pull(task_ids="prepare_features", key="feature_path")
    df = pd.read_parquet(feature_path)

    FEATURE_COLS = [
        "close", "volume", "trade_count",
        "rolling_vol_5m", "price_range_ratio", "vol_ratio",
        "avg_sentiment", "mention_count", "positive_ratio",
    ]
    TARGET_COL = "target_vol_5m"

    X = df[FEATURE_COLS].fillna(0)
    y = df[TARGET_COL].fillna(0)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    # ── MLflow tracking ─────────────────────────────────────
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    mlflow.set_experiment("btc-volatility-prediction")

    params = {
        "n_estimators":     200,
        "max_depth":        6,
        "learning_rate":    0.05,
        "subsample":        0.8,
        "colsample_bytree": 0.8,
        "random_state":     42,
        "tree_method":      "hist",
    }

    with mlflow.start_run(run_name=f"xgboost_{context['ds_nodash']}") as run:
        mlflow.log_params(params)
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("test_size", len(X_test))
        mlflow.log_param("features", FEATURE_COLS)

        model = XGBRegressor(**params)
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

        y_pred = model.predict(X_test)
        rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
        mae  = float(mean_absolute_error(y_test, y_pred))

        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)

        # Log feature importances
        for feat, imp in zip(FEATURE_COLS, model.feature_importances_):
            mlflow.log_metric(f"fi_{feat}", float(imp))

        # Log model
        mlflow.xgboost.log_model(
            model,
            artifact_path="model",
            registered_model_name="btc-volatility-xgboost",
        )

        run_id = run.info.run_id
        print(f"MLflow Run ID: {run_id}, RMSE: {rmse:.6f}, MAE: {mae:.6f}")

    # ── Promote model ke Production ─────────────────────────
    from mlflow.tracking import MlflowClient

    client = MlflowClient(tracking_uri=os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    latest = client.get_latest_versions("btc-volatility-xgboost", stages=["None"])
    if latest:
        client.transition_model_version_stage(
            name="btc-volatility-xgboost",
            version=latest[0].version,
            stage="Production",
            archive_existing_versions=True,
        )
        print(f"Model versi {latest[0].version} dipromosikan ke Production.")

    context["ti"].xcom_push(key="rmse", value=rmse)
    context["ti"].xcom_push(key="run_id", value=run_id)


# ─── Task: audit log ─────────────────────────────────────────
def log_training_result(**context):
    """Catat hasil training ke audit_log."""
    import psycopg2

    ti = context["ti"]
    rmse    = ti.xcom_pull(task_ids="train_model", key="rmse") or -1
    run_id  = ti.xcom_pull(task_ids="train_model", key="run_id") or ""
    samples = ti.xcom_pull(task_ids="prepare_features", key="sample_count") or 0

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
                    "MODEL_TRAINING",
                    "mlflow:btc-volatility-xgboost",
                    f"Training selesai. RMSE={rmse:.6f}, samples={samples}, "
                    f"MLflow run_id={run_id}, DAG run={context['run_id']}",
                ),
            )
            conn.commit()
        conn.close()
    except Exception as e:
        print(f"Warning: gagal catat audit log: {e}")


# ─── DAG Definition ──────────────────────────────────────────
with DAG(
    dag_id="model_training",
    description="Training XGBoost volatilitas BTC + MLflow tracking, mingguan",
    schedule_interval="0 2 * * 1",   # Senin 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ml", "xgboost", "mlflow", "training"],
    max_active_runs=1,
) as dag:

    check_task = BranchPythonOperator(
        task_id="check_data_availability",
        python_callable=check_training_data,
        provide_context=True,
    )

    skip_task = EmptyOperator(task_id="skip_training")

    prepare_task = PythonOperator(
        task_id="prepare_features",
        python_callable=prepare_features,
        provide_context=True,
    )

    train_task = PythonOperator(
        task_id="train_model",
        python_callable=train_xgboost,
        provide_context=True,
    )

    audit_task = PythonOperator(
        task_id="log_audit",
        python_callable=log_training_result,
        provide_context=True,
    )

    check_task >> [skip_task, prepare_task]
    prepare_task >> train_task >> audit_task