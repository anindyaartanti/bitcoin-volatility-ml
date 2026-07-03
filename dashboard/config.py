import os

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "btcdb")
PG_USER = os.getenv("PG_USER", "kelompok4_ipbd")
PG_PASSWORD = os.getenv("PG_PASSWORD", "k4ipbd_postgres_2026")

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8082"))
TRINO_USER = os.getenv("TRINO_USER", "kelompok4_ipbd")

DASHBOARD_TITLE = "Bitcoin Volatility ML — Dashboard"
DASHBOARD_REFRESH_SECONDS = 30
