import pandas as pd
from db import query_pg


def get_app_logs(service: str = None, level: str = None, limit: int = 200) -> pd.DataFrame:
    sql = "SELECT timestamp, service, level, message FROM app_logs"
    conditions = []
    if service:
        conditions.append(f"service = '{service}'")
    if level:
        conditions.append(f"level = '{level}'")
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY timestamp DESC LIMIT " + str(limit)
    return query_pg(sql)


def get_log_services() -> list:
    df = query_pg("SELECT DISTINCT service FROM app_logs ORDER BY service")
    return df["service"].tolist() if not df.empty else []


def get_log_level_counts() -> pd.DataFrame:
    return query_pg(
        """
        SELECT level, COUNT(*) AS count
        FROM app_logs
        WHERE timestamp > NOW() - INTERVAL '24 hours'
        GROUP BY level
        ORDER BY count DESC
        """
    )


def get_error_timeseries() -> pd.DataFrame:
    return query_pg(
        """
        SELECT date_trunc('hour', timestamp) AS hour, COUNT(*) AS errors
        FROM app_logs
        WHERE level IN ('ERROR','CRITICAL')
          AND timestamp > NOW() - INTERVAL '7 days'
        GROUP BY 1 ORDER BY 1
        """
    )


def get_pipeline_error_summary() -> pd.DataFrame:
    return query_pg(
        """
        SELECT pipeline_name AS service, COUNT(*) AS runs,
               SUM(CASE WHEN quality_status = 'failed' THEN 1 ELSE 0 END) AS failures,
               MAX(finished_at) AS last_run
        FROM pipeline_lineage
        WHERE finished_at > NOW() - INTERVAL '24 hours'
        GROUP BY pipeline_name
        ORDER BY failures DESC
        """
    )


def get_alert_history(limit: int = 30) -> pd.DataFrame:
    return query_pg(
        f"""
        SELECT timestamp, service, level, message
        FROM app_logs
        WHERE level IN ('ERROR','CRITICAL')
        ORDER BY timestamp DESC
        LIMIT {limit}
        """
    )
