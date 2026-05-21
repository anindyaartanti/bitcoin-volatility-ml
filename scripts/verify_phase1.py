"""
scripts/verify_phase1.py
========================
Script verifikasi otomatis untuk memastikan semua komponen Fase 1
berjalan dengan benar.

Cara pakai:
    python scripts/verify_phase1.py

Setiap pengecekan menampilkan ✅ (berhasil) atau ❌ (gagal).
"""

import json
import os
import socket
import sys
import time
from datetime import datetime, timezone
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ─── Konfigurasi ────────────────────────────────────────────
CONFIG = {
    "kafka_host":       os.getenv("KAFKA_HOST", "localhost"),
    "kafka_port":       int(os.getenv("KAFKA_PORT", 9092)),
    "postgres_host":    os.getenv("APP_DB_HOST", "localhost"),
    "postgres_port":    int(os.getenv("APP_DB_PORT", 5432)),
    "postgres_db":      os.getenv("APP_DB_NAME", "btcdb"),
    "postgres_user":    os.getenv("APP_DB_USER", "btcadmin"),
    "postgres_pass":    os.getenv("APP_DB_PASSWORD", "gantiPasswordAman123"),
    "minio_endpoint":   os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", ""),
    "minio_access":     os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "minio_secret":     os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
    "airflow_host":     os.getenv("AIRFLOW_HOST", "localhost"),
    "airflow_port":     int(os.getenv("AIRFLOW_PORT", 8080)),
}

results = []


def check(name: str, fn):
    try:
        fn()
        print(f"  ✅  {name}")
        results.append((name, True, None))
    except Exception as e:
        print(f"  ❌  {name}")
        print(f"       Error: {e}")
        results.append((name, False, str(e)))


def port_open(host: str, port: int, timeout: float = 3.0):
    s = socket.create_connection((host, port), timeout=timeout)
    s.close()


# ─── Pengecekan ──────────────────────────────────────────────

print("\n" + "="*55)
print("  VERIFIKASI FASE 1 - Bitcoin Volatility ML")
print("  " + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))
print("="*55)

# 1. Kafka
print("\n[1] Kafka Broker")
check("Kafka port 9092 terbuka", lambda: port_open(CONFIG["kafka_host"], CONFIG["kafka_port"]))

def check_kafka_topics():
    from kafka import KafkaAdminClient
    admin = KafkaAdminClient(
        bootstrap_servers=f"{CONFIG['kafka_host']}:{CONFIG['kafka_port']}",
        request_timeout_ms=5000,
    )
    topics = admin.list_topics()
    admin.close()
    assert "btc_ticker_raw" in topics, f"Topic 'btc_ticker_raw' tidak ditemukan. Topics: {topics}"

check("Topic 'btc_ticker_raw' ada", check_kafka_topics)

def check_kafka_data():
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        "btc_ticker_raw",
        bootstrap_servers=f"{CONFIG['kafka_host']}:{CONFIG['kafka_port']}",
        auto_offset_reset="latest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    msgs = []
    for msg in consumer:
        msgs.append(msg.value)
        if len(msgs) >= 1:
            break
    consumer.close()
    assert len(msgs) > 0, "Tidak ada data mengalir ke Kafka. Apakah binance-producer berjalan?"

check("Data mengalir ke 'btc_ticker_raw'", check_kafka_data)

# 2. PostgreSQL
print("\n[2] PostgreSQL")
check("PostgreSQL port 5432 terbuka", lambda: port_open(CONFIG["postgres_host"], CONFIG["postgres_port"]))

def check_postgres_tables():
    import psycopg2
    conn = psycopg2.connect(
        host=CONFIG["postgres_host"],
        port=CONFIG["postgres_port"],
        dbname=CONFIG["postgres_db"],
        user=CONFIG["postgres_user"],
        password=CONFIG["postgres_pass"],
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    tables = {row[0] for row in cur.fetchall()}
    conn.close()
    required = {"btc_ohlc_1m", "sentiment_hourly", "predictions", "metadata_table", "audit_log"}
    missing = required - tables
    assert not missing, f"Tabel hilang: {missing}"

check("Semua tabel wajib ada", check_postgres_tables)

def check_postgres_roles():
    import psycopg2
    conn = psycopg2.connect(
        host=CONFIG["postgres_host"],
        port=CONFIG["postgres_port"],
        dbname=CONFIG["postgres_db"],
        user=CONFIG["postgres_user"],
        password=CONFIG["postgres_pass"],
    )
    cur = conn.cursor()
    cur.execute("SELECT rolname FROM pg_roles WHERE rolname IN ('dashboard_reader', 'ml_writer')")
    roles = {row[0] for row in cur.fetchall()}
    conn.close()
    assert "dashboard_reader" in roles, "Role 'dashboard_reader' tidak ditemukan"
    assert "ml_writer" in roles, "Role 'ml_writer' tidak ditemukan"

check("Role 'dashboard_reader' & 'ml_writer' ada", check_postgres_roles)

# 3. MinIO
print("\n[3] MinIO")
check("MinIO port 9000 terbuka", lambda: port_open(
    CONFIG["minio_endpoint"].split(":")[0],
    int(CONFIG["minio_endpoint"].split(":")[1]) if ":" in CONFIG["minio_endpoint"] else 9000
))

def check_minio_buckets():
    from minio import Minio
    client = Minio(
        CONFIG["minio_endpoint"],
        access_key=CONFIG["minio_access"],
        secret_key=CONFIG["minio_secret"],
        secure=False,
    )
    buckets = {b.name for b in client.list_buckets()}
    required = {"twitter-raw", "mlflow-artifacts", "spark-checkpoints"}
    missing = required - buckets
    assert not missing, f"Bucket hilang: {missing}"

check("Semua bucket MinIO ada", check_minio_buckets)

# 4. Airflow
print("\n[4] Airflow")
check("Airflow WebUI port 8080 terbuka", lambda: port_open(CONFIG["airflow_host"], CONFIG["airflow_port"]))

def check_airflow_dag():
    import urllib.request
    url = f"http://{CONFIG['airflow_host']}:{CONFIG['airflow_port']}/health"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=5) as resp:
        data = json.loads(resp.read())
    scheduler_ok = data.get("scheduler", {}).get("status") == "healthy"
    db_ok        = data.get("metadatabase", {}).get("status") == "healthy"
    assert scheduler_ok and db_ok, f"Airflow tidak healthy: {data}"
check("Airflow health endpoint OK", check_airflow_dag)

# ─── Ringkasan ───────────────────────────────────────────────
print("\n" + "="*55)
passed = sum(1 for _, ok, _ in results if ok)
total  = len(results)
print(f"  Hasil: {passed}/{total} pengecekan berhasil")

if passed == total:
    print("  🎉 Fase 1 LULUS! Siap lanjut ke Fase 2.")
else:
    print("  ⚠️  Ada komponen yang belum siap. Cek error di atas.")
    failed = [(name, err) for name, ok, err in results if not ok]
    print("\n  Gagal:")
    for name, err in failed:
        print(f"    - {name}: {err}")

print("="*55 + "\n")
sys.exit(0 if passed == total else 1)
