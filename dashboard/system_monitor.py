"""
dashboard/system_monitor.py
============================
Query Telegraf metrics tables from PostgreSQL for System Health dashboard.
Telegraf auto-creates tables: cpu, mem, disk, docker_container_cpu,
docker_container_mem, docker_container_status
"""

import warnings
from datetime import datetime, timezone

import pandas as pd
from db import query_pg

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")


def _table_exists(table_name: str) -> bool:
    try:
        query_pg(f"SELECT 1 FROM {table_name} LIMIT 1")
        return True
    except Exception:
        return False


def get_host_cpu() -> pd.DataFrame:
    if not _table_exists("cpu"):
        return pd.DataFrame()
    return query_pg(
        """
        SELECT time, usage_idle, usage_system, usage_user
        FROM cpu
        WHERE time > NOW() - INTERVAL '24 hours'
        ORDER BY time
        """
    )


def get_host_memory() -> pd.DataFrame:
    if not _table_exists("mem"):
        return pd.DataFrame()
    return query_pg(
        """
        SELECT time, total, available, used, used_percent
        FROM mem
        WHERE time > NOW() - INTERVAL '24 hours'
        ORDER BY time
        """
    )


def get_host_memory_latest() -> dict:
    if not _table_exists("mem"):
        return {}
    df = query_pg("SELECT total, available, used, used_percent FROM mem ORDER BY time DESC LIMIT 1")
    if df.empty:
        return {}
    return {
        "total": float(df["total"].iloc[0]),
        "available": float(df["available"].iloc[0]),
        "used": float(df["used"].iloc[0]),
        "used_percent": float(df["used_percent"].iloc[0]),
    }


def get_host_disk() -> pd.DataFrame:
    if not _table_exists("disk"):
        return pd.DataFrame()
    return query_pg(
        """
        SELECT time, path, total, used, used_percent
        FROM disk
        WHERE time > NOW() - INTERVAL '24 hours'
        ORDER BY time
        """
    )


def get_host_disk_latest() -> dict:
    if not _table_exists("disk"):
        return {}
    df = query_pg(
        """
        SELECT path, total, used, used_percent
        FROM disk
        WHERE used_percent IS NOT NULL
          AND path NOT LIKE '/var/lib/docker/%'
        ORDER BY time DESC LIMIT 1
        """
    )
    if df.empty:
        return {}
    return {
        "path": str(df["path"].iloc[0]),
        "total": float(df["total"].iloc[0]),
        "used": float(df["used"].iloc[0]),
        "used_percent": float(df["used_percent"].iloc[0]),
    }


def get_container_cpu() -> pd.DataFrame:
    if not _table_exists("docker_container_cpu"):
        return pd.DataFrame()
    return query_pg(
        """
        SELECT time, container_name, usage_percent
        FROM docker_container_cpu
        WHERE time > NOW() - INTERVAL '15 minutes'
          AND container_name IS NOT NULL
        ORDER BY time
        """
    )


def get_container_memory() -> pd.DataFrame:
    if not _table_exists("docker_container_mem"):
        return pd.DataFrame()
    return query_pg(
        """
        SELECT time, container_name, usage_percent, usage, "limit"
        FROM docker_container_mem
        WHERE time > NOW() - INTERVAL '15 minutes'
          AND container_name IS NOT NULL
        ORDER BY time
        """
    )


def get_container_memory_latest() -> pd.DataFrame:
    if not _table_exists("docker_container_mem"):
        return pd.DataFrame()
    return query_pg(
        """
        SELECT DISTINCT ON (container_name)
            container_name,
            usage_percent,
            usage,
            "limit",
            time
        FROM docker_container_mem
        WHERE time > NOW() - INTERVAL '5 minutes'
          AND container_name IS NOT NULL
        ORDER BY container_name, time DESC
        """
    )


def get_container_status() -> pd.DataFrame:
    if not _table_exists("docker_container_status"):
        return pd.DataFrame()
    return query_pg(
        """
        SELECT DISTINCT ON (container_name)
            container_name,
            status,
            oomkilled,
            time
        FROM docker_container_status
        WHERE time > NOW() - INTERVAL '5 minutes'
          AND container_name IS NOT NULL
        ORDER BY container_name, time DESC
        """
    )


def get_cpu_latest() -> dict:
    if not _table_exists("cpu"):
        return {}
    df = query_pg("SELECT usage_idle, usage_system, usage_user FROM cpu ORDER BY time DESC LIMIT 1")
    if df.empty:
        return {}
    idle = float(df["usage_idle"].iloc[0])
    return {
        "usage_total": round(100 - idle, 1),
        "usage_system": float(df["usage_system"].iloc[0]),
        "usage_user": float(df["usage_user"].iloc[0]),
    }


def get_telegraf_available() -> bool:
    return _table_exists("cpu") and _table_exists("mem")
