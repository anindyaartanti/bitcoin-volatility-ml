"""
prefect/flows/data_quality_check.py
=====================================
Prefect flow: data-quality-check
- Profile semua tabel (null %, distinct, min/max/mean)
- Jalankan Great Expectations checkpoint per tabel
- Simpan hasil ke data_quality_stats + pipeline_lineage
- Telegram alert jika threshold breach
- Schedule: setiap 1 jam
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

import pandas as pd
import psycopg2
import requests
from prefect import flow, task
from prefect.client.schemas.schedules import IntervalSchedule

logger = logging.getLogger("data_quality_check")

PGBOUNCER_HOST = os.getenv("PGBOUNCER_HOST", "pgbouncer")
PGBOUNCER_PORT = int(os.getenv("PGBOUNCER_PORT", "6432"))
PG_DIRECT_HOST = os.getenv("APP_DB_HOST", "postgres")
PG_DIRECT_PORT = int(os.getenv("APP_DB_PORT", "5432"))
PG_DB = os.getenv("APP_DB_NAME", "btcdb")
PG_USER = os.getenv("APP_DB_USER", "kelompok4_ipbd")
PG_PASSWORD = os.getenv("APP_DB_PASSWORD", "")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

TABLES_TO_PROFILE = [
    "btc_ohlc_1m",
    "sentiment_30m",
    "volatility_pred",
    "pipeline_lineage",
]

NULL_THRESHOLD_PCT = 10.0

DB_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_DIRECT_HOST}:{PG_DIRECT_PORT}/{PG_DB}"


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


def _load_ge_context():
    """Load GE context from bundled config inside prefect container."""
    import great_expectations as gx
    ge_root = "/opt/prefect/ge"
    context = gx.get_context(
        project_root_dir=ge_root,
        project_config=os.path.join(ge_root, "great_expectations.yml"),
    )
    return context


@task(retries=2, retry_delay_seconds=60)
def profile_tables() -> dict:
    """Profile all tables: compute null %, distinct, min/max/mean."""
    engine = None
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(DB_URL)
        results = []
        alerts = []

        for table in TABLES_TO_PROFILE:
            with engine.begin() as conn:
                df = pd.read_sql(f"SELECT * FROM {table} LIMIT 10000", conn)
                total_rows = len(df)
                if total_rows == 0:
                    logger.info(f"{table}: empty, skip profiling")
                    results.append({
                        "table": table, "total_rows": 0,
                        "columns": [], "alerts": [],
                    })
                    continue

                numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
                col_stats = []
                for col in df.columns:
                    null_count = int(df[col].isnull().sum())
                    null_pct = round(null_count / total_rows * 100, 2)
                    distinct = int(df[col].nunique()) if total_rows > 0 else 0
                    type_mismatch = 0

                    min_v, max_v, mean_v = None, None, None
                    if col in numeric_cols and not df[col].isnull().all():
                        try:
                            s = pd.to_numeric(df[col], errors="coerce")
                            type_mismatch = int(s.isnull().sum() - null_count)
                            valid = s.dropna()
                            if len(valid) > 0:
                                min_v = float(valid.min())
                                max_v = float(valid.max())
                                mean_v = float(valid.mean())
                        except Exception:
                            pass

                    col_stats.append({
                        "column": col, "null_count": null_count,
                        "null_pct": null_pct, "distinct": distinct,
                        "type_mismatch": type_mismatch,
                        "min_value": min_v, "max_value": max_v, "mean_value": mean_v,
                    })

                    quality = "ok" if null_pct < NULL_THRESHOLD_PCT else "failed"
                    if null_pct >= NULL_THRESHOLD_PCT:
                        alerts.append(
                            f"  - {table}.{col}: null {null_pct:.1f}% (threshold {NULL_THRESHOLD_PCT}%)"
                        )

                results.append({
                    "table": table, "total_rows": total_rows,
                    "columns": col_stats, "alerts": alerts,
                })

        if alerts:
            _send_telegram(
                f"<b>[DATA QUALITY] Null threshold breach:</b>\n" +
                "\n".join(alerts[:5])
            )

        return {"tables": results, "alerts": alerts}
    finally:
        if engine:
            engine.dispose()


@task(retries=2, retry_delay_seconds=60)
def store_profiling_results(profile_result: dict) -> int:
    """Store profiling results into data_quality_stats table."""
    conn = _pg_conn()
    rows_inserted = 0
    try:
        with conn.cursor() as cur:
            for table_info in profile_result["tables"]:
                table_name = table_info["table"]
                total_rows = table_info["total_rows"]
                for col in table_info["columns"]:
                    cur.execute(
                        """
                        INSERT INTO data_quality_stats
                            (table_name, column_name, null_count, total_rows,
                             null_percent, distinct_count, type_mismatch,
                             min_value, max_value, mean_value, quality_status, checked_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        """,
                        (
                            table_name, col["column"],
                            col["null_count"], total_rows,
                            col["null_pct"], col["distinct"],
                            col["type_mismatch"],
                            col["min_value"], col["max_value"], col["mean_value"],
                            "ok" if col["null_pct"] < NULL_THRESHOLD_PCT else "failed",
                        ),
                    )
                    rows_inserted += 1
        conn.commit()
    finally:
        conn.close()
    return rows_inserted


@task(retries=1)
def run_ge_validation() -> dict:
    """Run Great Expectations checkpoint for all suites."""
    passed = 0
    failed = 0
    details = {}

    try:
        context = _load_ge_context()

        for suite_name in ["btc_ohlc_1m_suite", "sentiment_30m_suite", "volatility_pred_suite"]:
            try:
                suite = context.get_expectation_suite(suite_name)

                table_name = suite_name.replace("_suite", "")
                batch_request = context.get_datasource("postgres_btcdb") \
                    .get_asset(table_name) if hasattr(context.get_datasource("postgres_btcdb"), "get_asset") else None

                validator = context.get_validator(
                    batch_request=batch_request,
                    expectation_suite=suite,
                ) if batch_request else None

                if validator is None:
                    # Fallback: query table directly to create runtime batch
                    import pandas as pd
                    from sqlalchemy import create_engine
                    engine = create_engine(DB_URL)
                    df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5000", engine)
                    engine.dispose()

                    validator = context.get_validator(
                        batch_request={
                            "datasource_name": "postgres_btcdb",
                            "data_connector_name": "default_runtime_data_connector",
                            "data_asset_name": table_name,
                            "runtime_parameters": {"batch_data": df},
                            "batch_identifiers": {"default_identifier_name": "default"},
                        },
                        expectation_suite=suite,
                    )

                result = validator.validate()
                stats = result["statistics"]
                table_passed = stats["successful_expectations"]
                table_failed = stats["unsuccessful_expectations"]
                passed += table_passed
                failed += table_failed
                details[table_name] = {
                    "success": result["success"],
                    "passed": table_passed,
                    "failed": table_failed,
                }
                logger.info(
                    "GE %s: passed=%d failed=%d success=%s",
                    table_name, table_passed, table_failed, result["success"],
                )
            except Exception as e:
                logger.warning("GE validation gagal untuk %s: %s", suite_name, e)
                details[suite_name] = {"success": False, "error": str(e)}
                failed += 1

    except Exception as e:
        logger.warning("GE context gagal diinisialisasi: %s", e)
        details["ge_init"] = {"success": False, "error": str(e)}

    return {
        "checks_passed": passed,
        "checks_failed": failed,
        "details": details,
    }


@task(retries=2, retry_delay_seconds=30)
def record_lineage_dq(ge_result: dict, profile_alerts: list, rows_stored: int) -> None:
    """Record data quality run in pipeline_lineage."""
    quality_status = "ok" if ge_result["checks_failed"] == 0 else "failed"
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_lineage
                    (pipeline_name, source, target_table, rows_processed,
                     rows_rejected, quality_status, started_at, finished_at, params)
                VALUES (%s, %s, %s, %s, %s, %s,
                        NOW() - INTERVAL '5 minutes', NOW(), %s)
                """,
                (
                    "data_quality_check",
                    "btc_ohlc_1m + sentiment_30m + volatility_pred",
                    "data_quality_stats",
                    rows_stored,
                    0,
                    quality_status,
                    json.dumps({
                        "checks_passed": ge_result["checks_passed"],
                        "checks_failed": ge_result["checks_failed"],
                        "profile_alerts_count": len(profile_alerts),
                        "details": ge_result.get("details", {}),
                    }),
                ),
            )
        conn.commit()
    finally:
        conn.close()

    if ge_result["checks_failed"] > 0:
        _send_telegram(
            f"<b>[DATA QUALITY] {ge_result['checks_failed']} validasi gagal!</b>\n"
            f"Passed: {ge_result['checks_passed']}\n"
            f"Cek pipeline_lineage untuk detail."
        )


@flow(name="data-quality-check", log_prints=True)
def data_quality_check() -> None:
    profile_result = profile_tables()
    rows_stored = store_profiling_results(profile_result)
    ge_result = run_ge_validation()
    record_lineage_dq(ge_result, profile_result.get("alerts", []), rows_stored)


def deploy() -> None:
    from prefect.deployments import Deployment
    from prefect.client.schemas.objects import MinimalDeploymentSchedule
    from prefect.client.schemas.schedules import IntervalSchedule

    Deployment.build_from_flow(
        flow=data_quality_check,
        name="data-quality-check-hourly",
        work_pool_name="default",
        schedules=[MinimalDeploymentSchedule(
            schedule=IntervalSchedule(interval=3600)
        )],
        apply=True,
    )
    print("Deployment 'data-quality-check-hourly' created.")
