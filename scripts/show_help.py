"""
scripts/show_help.py
====================
Tampilkan daftar perintah make - dipanggil oleh `make help`.
"""

commands = {
    "STACK MANAGEMENT": [
        ("make up",                "Jalankan semua service"),
        ("make up-infra",          "Jalankan hanya Kafka + PostgreSQL + MinIO"),
        ("make down",              "Hentikan semua service (data tetap ada)"),
        ("make down-volumes",      "Hentikan + hapus semua volume (DATA HILANG!)"),
        ("make restart",           "Restart semua service"),
        ("make ps",                "Tampilkan status semua container"),
        ("make logs",              "Log semua service (live, Ctrl+C untuk stop)"),
        ("make logs-kafka",        "Log Kafka saja"),
        ("make logs-airflow",      "Log Airflow scheduler saja"),
        ("make logs-producer",     "Log Binance WebSocket producer saja"),
    ],
    "SETUP": [
        ("make setup",             "Setup awal: buat .env + generate Fernet key"),
        ("make build",             "Build semua custom Docker image"),
        ("make airflow-init",      "Inisialisasi database Airflow + buat admin user"),
    ],
    "TESTING & VERIFIKASI": [
        ("make verify",            "Jalankan verifikasi Fase 1 lengkap"),
        ("make test-kafka",        "Tampilkan daftar topik Kafka"),
        ("make test-kafka-consume","Baca 10 pesan dari topik btc_ticker_raw"),
        ("make test-minio",        "Tampilkan daftar bucket MinIO"),
        ("make test-postgres",     "Tampilkan daftar tabel di PostgreSQL"),
        ("make test-twitter",      "Test scraping Twitter (5 tweet)"),
    ],
    "PRODUCER": [
        ("make start-producer",    "Jalankan Binance WebSocket producer"),
        ("make stop-producer",     "Hentikan Binance WebSocket producer"),
    ],
    "AIRFLOW": [
        ("make trigger-dag",       "Trigger DAG twitter_ingestion secara manual"),
        ("make list-dags",         "Tampilkan semua DAG yang terdaftar"),
        ("make dag-status",        "Cek 5 run terakhir DAG twitter_ingestion"),
    ],
    "INFO": [
        ("make open-all",          "Tampilkan semua URL service"),
        ("make help",              "Tampilkan daftar perintah ini"),
    ],
    "CLEANUP": [
        ("make clean-logs",        "Hapus log Airflow yang lebih dari 7 hari"),
        ("make clean",             "Hapus container stop + image tidak terpakai"),
    ],
}

print()
print("=" * 62)
print("  Bitcoin Volatility ML - Daftar Perintah Make")
print("=" * 62)

for section, items in commands.items():
    print()
    print(f"  [{section}]")
    for cmd, desc in items:
        print(f"    {cmd:<30} {desc}")

print()
print("=" * 62)
print()