import json
import logging
import os
from datetime import datetime, timezone

import psycopg2
import requests
from prefect import flow, task
from prefect.client.schemas.schedules import IntervalSchedule

logger = logging.getLogger("system_health_check")

PGBOUNCER_HOST = os.getenv("PGBOUNCER_HOST", "pgbouncer")
PGBOUNCER_PORT = int(os.getenv("PGBOUNCER_PORT", "6432"))
PG_DB = os.getenv("APP_DB_NAME", "btcdb")
PG_USER = os.getenv("APP_DB_USER", "kelompok4_ipbd")
PG_PASSWORD = os.getenv("APP_DB_PASSWORD", "")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

CRITICAL_CONTAINERS = [
    "postgres", "kafka", "zookeeper", "minio", "spark-master",
    "spark-streaming-job", "binance-producer", "pgbouncer", "trino",
]

MEMORY_THRESHOLD = 85
DISK_THRESHOLD = 90
CPU_THRESHOLD = 90


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


@task(retries=1)
def check_containers() -> dict:
    import docker
    client = docker.from_env()
    containers = client.containers.list(all=True)
    result = {"total": len(containers), "running": 0, "stopped": [], "restarting": []}

    for c in containers:
        status = c.status
        name = c.name
        if status == "running":
            result["running"] += 1
        elif status == "exited":
            if name in CRITICAL_CONTAINERS:
                result["stopped"].append(name)
                _send_telegram(
                    f"<b>[SYSTEM] Container CRITICAL mati:</b> {name}\n"
                    f"Status: {status}\n"
                    f"Image: {c.image.tags[0] if c.image.tags else 'unknown'}"
                )
            else:
                result["stopped"].append(name)
        elif status == "restarting":
            result["restarting"].append(name)
            _send_telegram(
                f"<b>[SYSTEM] Container restart-loop:</b> {name}"
            )

    return result


@task(retries=1)
def check_host_resources() -> dict:
    import psutil
    result = {
        "cpu_percent": psutil.cpu_percent(interval=1),
        "memory_total_mb": psutil.virtual_memory().total // (1024 * 1024),
        "memory_used_mb": psutil.virtual_memory().used // (1024 * 1024),
        "memory_percent": psutil.virtual_memory().percent,
        "disk_total_gb": psutil.disk_usage("/").total // (1024 ** 3),
        "disk_used_gb": psutil.disk_usage("/").used // (1024 ** 3),
        "disk_percent": psutil.disk_usage("/").percent,
    }

    alerts = []
    if result["memory_percent"] > MEMORY_THRESHOLD:
        alerts.append(f"Memory {result['memory_percent']}% > {MEMORY_THRESHOLD}%")
    if result["disk_percent"] > DISK_THRESHOLD:
        alerts.append(f"Disk {result['disk_percent']}% > {DISK_THRESHOLD}%")
    if result["cpu_percent"] > CPU_THRESHOLD:
        alerts.append(f"CPU {result['cpu_percent']}% > {CPU_THRESHOLD}%")

    if alerts:
        _send_telegram(
            f"<b>[SYSTEM] Resource threshold breached:</b>\n" +
            "\n".join(f"  - {a}" for a in alerts)
        )

    return result


@task(retries=1)
def record_health_check(container_result: dict, resource_result: dict) -> None:
    try:
        conn = psycopg2.connect(
            host=PGBOUNCER_HOST, port=PGBOUNCER_PORT,
            dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_lineage
                    (pipeline_name, source, target_table, rows_processed,
                     rows_rejected, quality_status, started_at, finished_at, params)
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), %s)
                """,
                (
                    "system_health_check",
                    "docker_socket + psutil",
                    "pipeline_lineage",
                    1, 0,
                    "ok" if not container_result["stopped"] else "failed",
                    json.dumps({
                        "containers_total": container_result["total"],
                        "containers_running": container_result["running"],
                        "stopped_critical": container_result["stopped"],
                        "restarting": container_result["restarting"],
                        "cpu_percent": resource_result["cpu_percent"],
                        "memory_percent": resource_result["memory_percent"],
                        "disk_percent": resource_result["disk_percent"],
                    }),
                ),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Gagal record health check: %s", e)


@flow(name="system-health-check", log_prints=True)
def system_health_check() -> None:
    container_result = check_containers()
    resource_result = check_host_resources()
    record_health_check(container_result, resource_result)


def deploy() -> None:
    from prefect.deployments import Deployment
    from prefect.client.schemas.objects import MinimalDeploymentSchedule
    from prefect.client.schemas.schedules import IntervalSchedule

    Deployment.build_from_flow(
        flow=system_health_check,
        name="system-health-check",
        work_pool_name="default",
        schedules=[MinimalDeploymentSchedule(
            schedule=IntervalSchedule(interval=60)
        )],
        apply=True,
    )
    print("Deployment 'system-health-check' created.")
