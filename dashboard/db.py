import pandas as pd
from typing import Optional
import streamlit as st
from config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD, TRINO_HOST, TRINO_PORT, TRINO_USER


# ─── PostgreSQL (langsung, untuk data real-time/sederhana) ────
@st.cache_resource
def _pg_conn():
    import psycopg2
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
        connect_timeout=5,
    )


def query_pg(sql: str) -> pd.DataFrame:
    conn = _pg_conn()
    return pd.read_sql(sql, conn)


# ─── Trino (federated, untuk cross-source analytics) ──────────
@st.cache_resource
def _trino_conn():
    from trino.dbapi import connect
    return connect(
        host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER,
    )


def query_trino(sql: str) -> pd.DataFrame:
    conn = _trino_conn()
    cur = conn.cursor()
    cur.execute(sql)
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()
    return pd.DataFrame(rows, columns=cols)


# ─── Convenience ──────────────────────────────────────────────
def paginate_df(df: pd.DataFrame, page_size: int = 25):
    start = st.session_state.get("page", 0) * page_size
    return df.iloc[start:start + page_size]
