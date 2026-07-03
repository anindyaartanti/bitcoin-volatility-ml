import pandas as pd
from db import query_pg


def get_table_metadata() -> pd.DataFrame:
    return query_pg(
        """
        SELECT table_name, description, owner, sensitivity,
               refresh_frequency, source_system, retention_days
        FROM table_metadata
        ORDER BY table_name
        """
    )


def get_column_info(table_name: str) -> pd.DataFrame:
    return query_pg(
        f"""
        SELECT
            c.column_name,
            c.data_type,
            c.is_nullable,
            pg_catalog.col_description(
                (SELECT oid FROM pg_class WHERE relname = '{table_name}'),
                c.ordinal_position
            ) AS comment
        FROM information_schema.columns c
        WHERE c.table_schema = 'public'
          AND c.table_name = '{table_name}'
        ORDER BY c.ordinal_position
        """
    )


def get_all_tables() -> list:
    df = query_pg(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    )
    return df["table_name"].tolist() if not df.empty else []


def get_business_glossary() -> pd.DataFrame:
    return query_pg(
        """
        SELECT term, definition, technical_table, technical_column,
               category, owner
        FROM business_glossary
        ORDER BY category, term
        """
    )


def get_lineage_edges() -> list:
    sources = {
        "btc_ohlc_1m":     "Kafka (btc_ticker_raw)",
        "sentiment_30m":   "MinIO (twitter-raw)",
        "volatility_pred": "Spark (XGBoost inference)",
        "btc_predictions": "Prefect (model training)",
        "pipeline_lineage":"All pipeline runs",
        "audit_log":       "PostgreSQL triggers",
        "data_quality_stats": "Prefect (data-quality-check)",
    }
    consumers = {
        "btc_ohlc_1m":     ["v_ml_features", "Streamlit (Market)", "Grafana"],
        "sentiment_30m":   ["v_ml_features", "Streamlit (Sentiment)", "Grafana"],
        "volatility_pred": ["Streamlit (Volatility)", "Grafana", "Telegram (alert)"],
        "btc_predictions": ["MLflow", "Streamlit"],
        "pipeline_lineage":["Streamlit (Pipeline)"],
        "audit_log":       ["Streamlit (Pipeline)"],
        "data_quality_stats": ["Streamlit (Data Quality)"],
    }

    edges = []
    for table, source in sources.items():
        edges.append({"source": source, "target": table})
    for table, targets in consumers.items():
        for target in targets:
            edges.append({"source": table, "target": target})

    return edges


def get_column_lineage(table_name: str = None) -> pd.DataFrame:
    sql = """
        SELECT source_table, source_column, target_table, target_column,
               transformation, pipeline_name
        FROM column_lineage
    """
    if table_name:
        sql += f" WHERE target_table = '{table_name}' OR source_table = '{table_name}'"
    sql += " ORDER BY pipeline_name, source_table, target_column"
    return query_pg(sql)


def get_upstream_columns(table_name: str) -> pd.DataFrame:
    return query_pg(
        f"""
        SELECT source_table, source_column, target_column, transformation
        FROM column_lineage
        WHERE target_table = '{table_name}'
        ORDER BY target_column
        """
    )


def get_downstream_columns(table_name: str) -> pd.DataFrame:
    return query_pg(
        f"""
        SELECT target_table, target_column, source_column, transformation
        FROM column_lineage
        WHERE source_table = '{table_name}'
        ORDER BY target_table, target_column
        """
    )
