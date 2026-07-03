import pandas as pd
from db import query_pg


def get_pred_vs_actual(hours: int = 24) -> pd.DataFrame:
    return query_pg(
        f"""
        SELECT window_start, predicted_vol, actual_vol, model_version
        FROM btc_predictions
        WHERE window_start > NOW() - INTERVAL '{hours} hours'
          AND actual_vol IS NOT NULL
        ORDER BY window_start
        """
    )


def get_model_performance_history() -> pd.DataFrame:
    return query_pg(
        """
        SELECT window_end AS checked_at, model_version, rmse, mae, n_predictions
        FROM model_performance
        ORDER BY checked_at DESC
        LIMIT 50
        """
    )


def get_latest_model_performance() -> dict:
    df = query_pg(
        """
        SELECT rmse, mae, n_predictions, model_version
        FROM model_performance
        ORDER BY checked_at DESC LIMIT 1
        """
    )
    if df.empty:
        return {}
    return {
        "rmse": float(df["rmse"].iloc[0]),
        "mae": float(df["mae"].iloc[0]),
        "n": int(df["n_predictions"].iloc[0]),
        "model_version": str(df["model_version"].iloc[0]),
    }


def get_residuals() -> pd.DataFrame:
    df = get_pred_vs_actual(24)
    if not df.empty:
        df["residual"] = df["actual_vol"].astype(float) - df["predicted_vol"].astype(float)
    return df
