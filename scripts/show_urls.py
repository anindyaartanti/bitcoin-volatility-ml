"""
scripts/show_urls.py
====================
Tampilkan semua URL akses service - dipanggil oleh `make open-all`.
"""

services = [
    ("Airflow WebUI",    "http://localhost:8080",  "airflow / airflow"),
    ("MinIO Console",    "http://localhost:9001",  "minioadmin / minioadmin123"),
    ("MLflow UI",        "http://localhost:5000",  "(tanpa login)"),
    ("Spark Master UI",  "http://localhost:8081",  "(tanpa login)"),
    ("Grafana",          "http://localhost:3000",  "admin / admin"),
    ("Streamlit",        "https://localhost/dashboard", "lihat .htpasswd"),
]

infra = [
    ("Kafka",      "localhost:9092"),
    ("PostgreSQL", "localhost:5432"),
    ("MinIO API",  "localhost:9000"),
]

print()
print("=" * 55)
print("  Bitcoin Volatility ML - URL Service")
print("=" * 55)
print()
print("  DASHBOARD & UI:")
for name, url, login in services:
    print(f"  {name:<20} {url}")
    print(f"  {'':20} Login: {login}")
    print()

print("  INFRASTRUKTUR (akses langsung):")
for name, addr in infra:
    print(f"  {name:<20} {addr}")

print()
print("  Catatan: Beberapa service (Grafana, Streamlit, MLflow)")
print("  baru tersedia mulai Fase 5-6.")
print("=" * 55)
print()