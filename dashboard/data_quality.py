import pandas as pd
from db import query_pg


def get_latest_dq_stats() -> pd.DataFrame:
    return query_pg(
        """
        SELECT DISTINCT ON (table_name, column_name)
            table_name, column_name, null_count, total_rows,
            null_percent, distinct_count, type_mismatch,
            min_value, max_value, mean_value, quality_status, checked_at
        FROM data_quality_stats
        ORDER BY table_name, column_name, checked_at DESC
        """
    )


def get_dq_summary() -> pd.DataFrame:
    return query_pg(
        """
        WITH latest AS (
            SELECT DISTINCT ON (table_name, column_name)
                table_name, column_name, null_percent, distinct_count,
                total_rows, quality_status, checked_at
            FROM data_quality_stats
            ORDER BY table_name, column_name, checked_at DESC
        )
        SELECT
            table_name,
            ROUND(AVG(100.0 - COALESCE(null_percent, 0)), 1) AS completeness_pct,
            COUNT(*) AS columns_checked,
            MAX(checked_at) AS last_checked,
            COUNT(*) FILTER (WHERE quality_status != 'ok') AS failed_columns,
            SUM(COALESCE(type_mismatch, 0)) AS total_type_errors
        FROM latest
        GROUP BY table_name
        ORDER BY table_name
        """
    )


def get_dq_history() -> pd.DataFrame:
    return query_pg(
        """
        SELECT started_at, quality_status, params->>'table_name' AS table_name,
               params->>'checks_passed' AS checks_passed,
               params->>'checks_failed' AS checks_failed
        FROM pipeline_lineage
        WHERE pipeline_name = 'data_quality_check'
        ORDER BY started_at DESC
        LIMIT 50
        """
    )


def get_null_heatmap() -> pd.DataFrame:
    return query_pg(
        """
        SELECT DISTINCT ON (table_name, column_name)
            table_name, column_name, null_percent
        FROM data_quality_stats
        ORDER BY table_name, column_name, checked_at DESC
        """
    )


def get_freshness() -> pd.DataFrame:
    queries = {
        "btc_ohlc_1m":     "SELECT MAX(window_start) AS last_row FROM btc_ohlc_1m",
        "sentiment_30m":   "SELECT MAX(window_start) AS last_row FROM sentiment_30m",
        "volatility_pred": "SELECT MAX(window_start) AS last_row FROM volatility_pred",
        "pipeline_lineage":"SELECT MAX(finished_at) AS last_row FROM pipeline_lineage",
        "audit_log":       "SELECT MAX(changed_at) AS last_row FROM audit_log",
    }
    rows = []
    for table, sql in queries.items():
        try:
            df = query_pg(sql)
            if not df.empty and df["last_row"].notna().any():
                rows.append({
                    "table_name": table,
                    "last_data_at": df["last_row"].iloc[0],
                })
            else:
                rows.append({"table_name": table, "last_data_at": None})
        except Exception:
            rows.append({"table_name": table, "last_data_at": None})
    return pd.DataFrame(rows)
