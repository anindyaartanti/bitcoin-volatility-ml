import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from prefect import flow, task

load_dotenv()


@task
def check_data_availability() -> dict:
    conn = psycopg2.connect(
        host=os.getenv("APP_DB_HOST", "postgres"),
        port=int(os.getenv("APP_DB_PORT", "5432")),
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
    return {"ohlc_count": ohlc_count, "sentiment_count": sentiment_count}


@task
def prepare_features() -> str:
    conn = psycopg2.connect(
        host=os.getenv("APP_DB_HOST", "postgres"),
        port=int(os.getenv("APP_DB_PORT", "5432")),
        dbname=os.getenv("APP_DB_NAME", "btcdb"),
        user=os.getenv("APP_DB_USER", "btcadmin"),
        password=os.getenv("APP_DB_PASSWORD", ""),
    )

    query = """
        WITH ohlc_features AS (
            SELECT
                window_start,
                open, high, low, close, volume, trade_count,
                STDDEV(
                    LN(close / LAG(close) OVER (ORDER BY window_start))
                ) OVER (ORDER BY window_start ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
                    AS rolling_vol_5m,
                (high - low) / NULLIF(open, 0) AS price_range_ratio,
                volume / NULLIF(LAG(volume) OVER (ORDER BY window_start), 0) AS vol_ratio,
                LEAD(
                    STDDEV(LN(close / LAG(close) OVER (ORDER BY window_start))
                    ) OVER (ORDER BY window_start ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
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

    tmp_path = "/tmp/btc_training_features.parquet"
    df.to_parquet(tmp_path, index=False)
    print(f"Feature disimpan di {tmp_path}")
    return tmp_path


@task
def train_model(feature_path: str) -> dict:
    import mlflow
    import mlflow.xgboost
    import numpy as np
    from sklearn.metrics import mean_absolute_error, mean_squared_error
    from sklearn.model_selection import train_test_split
    from xgboost import XGBRegressor

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

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    mlflow.set_experiment("btc-volatility-prediction")

    params = {
        "n_estimators": 200,
        "max_depth": 6,
        "learning_rate": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42,
        "tree_method": "hist",
    }

    with mlflow.start_run(run_name="xgboost_training") as run:
        mlflow.log_params(params)
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("test_size", len(X_test))
        mlflow.log_param("features", FEATURE_COLS)

        model = XGBRegressor(**params)
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

        y_pred = model.predict(X_test)
        rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
        mae = float(mean_absolute_error(y_test, y_pred))

        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)

        for feat, imp in zip(FEATURE_COLS, model.feature_importances_):
            mlflow.log_metric(f"fi_{feat}", float(imp))

        mlflow.xgboost.log_model(
            model,
            artifact_path="model",
            registered_model_name="btc-volatility-xgboost",
        )

        run_id = run.info.run_id
        print(f"MLflow Run ID: {run_id}, RMSE: {rmse:.6f}, MAE: {mae:.6f}")

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

    return {"rmse": rmse, "run_id": run_id, "samples": len(df)}


@task
def log_audit(result: dict):
    rmse = result.get("rmse", -1)
    run_id = result.get("run_id", "")
    samples = result.get("samples", 0)

    try:
        conn = psycopg2.connect(
            host=os.getenv("APP_DB_HOST", "postgres"),
            port=int(os.getenv("APP_DB_PORT", "5432")),
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
                    "prefect",
                    "MODEL_TRAINING",
                    "mlflow:btc-volatility-xgboost",
                    f"Training selesai. RMSE={rmse:.6f}, samples={samples}, "
                    f"MLflow run_id={run_id}",
                ),
            )
            conn.commit()
        conn.close()
    except Exception as e:
        print(f"Warning: gagal catat audit log: {e}")


@flow(log_prints=True)
def model_training_flow():
    counts = check_data_availability()
    if counts["ohlc_count"] < 1000 or counts["sentiment_count"] < 24:
        print("Data tidak cukup untuk training. Melewati model_training.")
        return

    feature_path = prepare_features()
    result = train_model(feature_path)
    log_audit(result)
    print(f"Model training selesai: {result}")


if __name__ == "__main__":
    model_training_flow()
