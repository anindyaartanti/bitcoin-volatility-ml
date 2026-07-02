"""
prefect/flows/log_ingester.py
===============================
Prefect flow: log-ingester
- Read recent pipeline_logs (pipeline_lineage + audit_log + system events)
- Aggregate log-level stats: ERROR/WARNING count per service per hour
- Store structured summary ke app_logs
- Schedule: setiap 5 menit
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta

import psycopg2
import requests
from prefect import flow, task
from prefect.client.schemas.schedules import IntervalSchedule

logger = logging.getLogger("log_ingester")

PGBOUNCER_HOST = os.getenv("PGBOUNCER_HOST", "pgbouncer")
PGBOUNCER_PORT = int(os.getenv("PGBOUNCER_PORT", "6432"))
PG_DB = os.getenv("APP_DB_NAME", "btcdb")
PG_USER = os.getenv("APP_DB_USER", "kelompok4_ipbd")
PG_PASSWORD = os.getenv("APP_DB_PASSWORD", "")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")


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
def collect_pipeline_events() -> dict:
    """Scan pipeline_lineage and audit_log for recent events, aggregate by service/level."""
    conn = _pg_conn()
    try:
        import pandas as pd
        df_lineage = pd.read_sql(
            """
            SELECT pipeline_name AS service, quality_status AS level,
                   finished_at AS timestamp, rows_processed, rows_rejected
            FROM pipeline_lineage
            WHERE finished_at > NOW() - INTERVAL '1 hour'
            """,
            conn,
        )
        df_audit = pd.read_sql(
            """
            SELECT table_name AS table_name, operation,
                   changed_at AS timestamp, changed_by
            FROM audit_log
            WHERE changed_at > NOW() - INTERVAL '1 hour'
            """,
            conn,
        )
    finally:
        conn.close()

    errors = len(df_lineage[df_lineage["level"] == "failed"]) if not df_lineage.empty else 0
    total_runs = len(df_lineage) if not df_lineage.empty else 0
    audit_events = len(df_audit) if not df_audit.empty else 0

    return {
        "pipeline_runs_1h": total_runs,
        "pipeline_failed_1h": errors,
        "audit_events_1h": audit_events,
    }


@task(retries=2, retry_delay_seconds=30)
def store_log_summary(stats: dict) -> None:
    now = datetime.now(timezone.utc)

    entries = []

    if stats["pipeline_runs_1h"] > 0:
        entries.append((
            "pipeline_orchestrator", "INFO",
            f"{stats['pipeline_runs_1h']} pipeline runs in last hour",
            json.dumps(stats),
        ))
    if stats["pipeline_failed_1h"] > 0:
        entries.append((
            "pipeline_orchestrator", "ERROR",
            f"{stats['pipeline_failed_1h']} pipeline failures in last hour",
            json.dumps(stats),
        ))
    entries.append((
        "pipeline_orchestrator", "INFO",
        f"{stats['audit_events_1h']} audit events in last hour",
        json.dumps(stats),
    ))

    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            for service, level, message, extra in entries:
                cur.execute(
                    """
                    INSERT INTO app_logs (service, level, message, extra)
                    VALUES (%s, %s, %s, %s::jsonb)
                    """,
                    (service, level, message, extra),
                )
            # Cleanup old logs (> 30 days)
            cur.execute(
                "DELETE FROM app_logs WHERE timestamp < NOW() - INTERVAL '30 days'"
            )
        conn.commit()
    finally:
        conn.close()

    if stats["pipeline_failed_1h"] > 0:
        _send_telegram(
            f"<b>[LOG] {stats['pipeline_failed_1h']} pipeline failure(s) in last hour</b>\n"
            f"Total runs: {stats['pipeline_runs_1h']}"
        )

    logger.info("Stored %d log entries, deleted old logs.", len(entries))


@flow(name="log-ingester", log_prints=True)
def log_ingester() -> None:
    stats = collect_pipeline_events()
    store_log_summary(stats)


def deploy() -> None:
    from prefect.deployments import Deployment
    from prefect.client.schemas.objects import MinimalDeploymentSchedule

    Deployment.build_from_flow(
        flow=log_ingester,
        name="log-ingester-5min",
        work_pool_name="default",
        schedules=[MinimalDeploymentSchedule(
            schedule=IntervalSchedule(interval=300)
        )],
        apply=True,
    )
    print("Deployment 'log-ingester-5min' created.")
