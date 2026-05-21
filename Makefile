# =============================================================
# Makefile - Bitcoin Volatility ML
# Kompatibel: Windows (cmd.exe) + Linux/Mac
#
# Strategi kompatibilitas Windows:
#   - Hindari SEMUA sintaks bash (if/else, &&, ||, test, read)
#   - Semua logic kondisional → file .bat / .py terpisah
#   - Makefile hanya berisi perintah satu baris yang aman
# =============================================================

COMPOSE := docker compose
PROJECT := bitcoin-volatility-ml

.PHONY: up up-infra down down-volumes restart ps \
        logs logs-kafka logs-airflow logs-producer \
        setup build verify \
        test-kafka test-kafka-consume test-minio test-postgres test-twitter \
        start-producer stop-producer \
        airflow-init trigger-dag list-dags dag-status \
        open-all clean-logs clean help

# ─────────────────────────────────────────────────────────────
# STACK MANAGEMENT
# ─────────────────────────────────────────────────────────────

up:
	$(COMPOSE) up -d

up-infra:
	$(COMPOSE) up -d zookeeper kafka postgres minio

down:
	$(COMPOSE) down

down-volumes:
	$(COMPOSE) down -v

restart:
	$(COMPOSE) restart

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f

logs-kafka:
	$(COMPOSE) logs -f kafka

logs-airflow:
	$(COMPOSE) logs -f airflow-scheduler

logs-producer:
	$(COMPOSE) logs -f binance-producer

# ─────────────────────────────────────────────────────────────
# SETUP
# Semua logic (cek file, generate key) ada di setup.bat / setup.sh
# ─────────────────────────────────────────────────────────────

setup:
	python scripts/setup.py

build:
	$(COMPOSE) build --no-cache

# ─────────────────────────────────────────────────────────────
# TESTING & VERIFIKASI
# ─────────────────────────────────────────────────────────────

verify:
	uv run python scripts/verify_phase1.py

test-kafka:
	$(COMPOSE) exec kafka kafka-topics --list --bootstrap-server localhost:29092

test-kafka-consume:
	$(COMPOSE) exec kafka kafka-console-consumer --topic btc_ticker_raw --bootstrap-server localhost:29092 --max-messages 10 --from-beginning

test-minio:
	$(COMPOSE) exec minio mc alias set local http://localhost:9000 minioadmin minioadmin123
	$(COMPOSE) exec minio mc ls local/

test-postgres:
	$(COMPOSE) exec postgres psql -U btcadmin -d btcdb -c "\dt"

test-twitter:
	$(COMPOSE) exec airflow-scheduler python /opt/airflow/ingestion/twitter_batch.py --query "bitcoin" --limit 5

# ─────────────────────────────────────────────────────────────
# PRODUCER
# ─────────────────────────────────────────────────────────────

start-producer:
	$(COMPOSE) up -d binance-producer

stop-producer:
	$(COMPOSE) stop binance-producer

# ─────────────────────────────────────────────────────────────
# AIRFLOW
# ─────────────────────────────────────────────────────────────

airflow-init:
	$(COMPOSE) run --rm airflow-init

trigger-dag:
	$(COMPOSE) exec airflow-webserver airflow dags trigger twitter_ingestion

list-dags:
	$(COMPOSE) exec airflow-webserver airflow dags list

dag-status:
	$(COMPOSE) exec airflow-webserver airflow dags list-runs -d twitter_ingestion --limit 5

# ─────────────────────────────────────────────────────────────
# INFO & CLEANUP
# ─────────────────────────────────────────────────────────────

open-all:
	python scripts/show_urls.py

clean-logs:
	$(COMPOSE) exec airflow-scheduler find /opt/airflow/logs -name "*.log" -mtime +7 -delete

clean:
	docker system prune -f

help:
	python scripts/show_help.py