# Bitcoin Volatility ML

ML pipeline for Bitcoin volatility prediction with real-time streaming, federated analytics, and alerting.

## Architecture

```
Binance WebSocket → Kafka → Spark Streaming → PostgreSQL (OHLC + predictions)
                                                    │
Twitter sentiment ──► Prefect ──► MinIO (Parquet) ──┤
                                                    │
                         Trino (federated join: PG + Parquet)
                              │
                         Streamlit Dashboard
                              │
                         Nginx (HTTPS + Basic Auth)
```

## Prerequisites

- Docker & Docker Compose
- Git
- Tailscale (for split-machine only)

---

## Option A: Single Machine

Run everything on one laptop.

### 1. Clone & setup

```bash
git clone <repo-url>
cd bitcoin-volatility-ml
cp .env.example .env
# edit .env jika perlu (default sudah work)
```

### 2. Generate SSL certificate

```bash
docker run --rm -v "${PWD}/security/nginx:/certs" nginx:alpine \
  sh -c "apk add --no-cache openssl > /dev/null && \
  openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout /certs/nginx.key -out /certs/nginx.crt -subj '/CN=localhost'"
```

### 3. Start all services

```bash
docker compose up -d --build
```

### 4. Access

| Service     | URL                                   | Auth                  |
|-------------|---------------------------------------|-----------------------|
| Dashboard   | `https://localhost`                   | kelompok4_ipbd / k4ipbd_nginx_2026 |
| Grafana     | `http://localhost:3001`               | kelompok4_ipbd / k4ipbd_grafana_2026 |
| Trino       | `http://localhost:8082`               | user: kelompok4_ipbd |
| MinIO       | `http://localhost:9003`               | minioadmin / k4ipbd_minio_2026 |
| MLflow      | `http://localhost:5001`               | -                     |
| Prefect     | `http://localhost:4201`               | -                     |

---

## Option B: Split Machine (via Tailscale)

Two laptops, one network via Tailscale Magic DNS.

```
fatih-omen (heavy)                         anin (lightweight)
─────────────────────────                   ────────────────────────
postgres :5434                              nginx :80 / :443
trino    :8082                                ↓ proxy_pass
kafka    :9093                              dashboard :8501
minio    :9000
spark    :7077
mlflow   :5000
prefect  :4200
hive-metastore :9083
grafana  :3001
binance-producer
spark-streaming-job
```

### Setup Tailscale (both laptops)

```bash
tailscale up
tailscale ip
# Hasil: fatih-omen → 100.x.x.1, anin → 100.x.x.2
```

### Laptop 1 — fatih-omen (heavy)

```bash
git clone <repo-url>
cd bitcoin-volatility-ml
cp .env.example .env

# generate SSL cert (opsional — hanya untuk single-machine, Laptop 1 gak butuh nginx)
# docker run ... (skip, gak perlu)

# start semua service
docker compose up -d --build

# verifikasi
docker ps | grep -E "(postgres|trino)"
```

Expected: 18 containers running, **tanpa** dashboard dan nginx.

### Laptop 2 — anin (lightweight)

```bash
git clone <repo-url>
cd bitcoin-volatility-ml
cp .env.laptop2.example .env
# File .env sudah default ke fatih-omen, gak perlu diubah:
#   LAPTOP1_TAILSCALE_IP=fatih-omen
#   NGINX_AUTH_USER=kelompok4_ipbd
#   NGINX_AUTH_PASSWORD=k4ipbd_nginx_2026

# generate SSL cert
docker run --rm -v "${PWD}/security/nginx:/certs" nginx:alpine \
  sh -c "apk add --no-cache openssl > /dev/null && \
  openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout /certs/nginx.key -out /certs/nginx.crt -subj '/CN=localhost'"

# start dashboard + nginx (hanya 2 container)
docker compose -f docker-compose.laptop2.yml up -d --build

# verifikasi
docker ps
# → dashboard, nginx
```

### Access (Laptop 2 — anin)

| Service     | URL                                        | Auth                  |
|-------------|--------------------------------------------|-----------------------|
| Dashboard   | `https://localhost`                        | kelompok4_ipbd / k4ipbd_nginx_2026 |
| Grafana     | `http://fatih-omen:3001`                   | kelompok4_ipbd / k4ipbd_grafana_2026 |
| MinIO UI    | `http://fatih-omen:9001`                   | minioadmin / k4ipbd_minio_2026 |
| MLflow      | `http://fatih-omen:5000`                   | -                     |
| Prefect UI  | `http://fatih-omen:4200`                   | -                     |

> **Note:** Dashboard via `https://localhost` (Nginx di laptop 2 sendiri).
> Service lain akses langsung ke `fatih-omen` via Tailscale (tanpa VPN gak nyampe).

---

## Verify Pipeline

### 1. Cek data masuk

```sql
-- dari laptop mana pun yang bisa akses PostgreSQL fatih-omen:5434
SELECT COUNT(*), MAX(window_start) FROM btc_ohlc_1m;
SELECT COUNT(*), MAX(window_start) FROM btc_predictions;
SELECT COUNT(*), MAX(window_start) FROM sentiment_30m;
```

### 2. Cek Trino cross-source

```sql
-- dari dashboard → halaman Cross-Source Analytics
-- atau via Trino CLI langsung
SELECT o.window_start, o.close, t.compound
FROM postgresql.public.btc_ohlc_1m o
LEFT JOIN hive.twitter_raw.tweets t
  ON date_trunc('minute', o.window_start) = date_trunc('minute', t.created_at)
LIMIT 10;
```

### 3. Cek alerting Grafana

Buka `http://fatih-omen:3001` → Alerting → lihat status 4 alert rules.

---

## Env Files Reference

| File              | Untuk                              |
|-------------------|------------------------------------|
| `.env.example`    | Single machine / Laptop 1 (heavy)  |
| `.env.laptop2.example` | Laptop 2 (dashboard only)     |

### Key Vars

| Variable                | Default            | Deskripsi                     |
|-------------------------|--------------------|-------------------------------|
| `LAPTOP1_TAILSCALE_IP`  | `fatih-omen`       | Tailscale hostname Laptop 1   |
| `POSTGRES_USER`         | `kelompok4_ipbd`   | PostgreSQL user               |
| `POSTGRES_PASSWORD`     | `k4ipbd_postgres_2026` | PostgreSQL password       |
| `NGINX_AUTH_USER`       | `kelompok4_ipbd`   | Basic Auth username           |
| `NGINX_AUTH_PASSWORD`   | `k4ipbd_nginx_2026` | Basic Auth password           |
| `TELEGRAM_BOT_TOKEN`    | `your_token`       | Token bot Telegram alerting   |
| `TELEGRAM_CHAT_ID`      | `your_chat_id`     | Chat ID Telegram              |

---

## Port Reference

| Service        | Internal | Host     | Laptop 2 connect ke                    |
|----------------|----------|----------|----------------------------------------|
| PostgreSQL     | 5432     | 5434     | `fatih-omen:5434`                      |
| Trino          | 8080     | 8082     | `fatih-omen:8082`                      |
| MinIO S3       | 9000     | 9000     | `fatih-omen:9000`                      |
| MinIO Console  | 9001     | 9001     | `fatih-omen:9001`                      |
| Kafka          | 9092     | 9092     | `fatih-omen:9092`                      |
| Spark Master   | 8080     | 8081     | `fatih-omen:8081`                      |
| Spark Cluster  | 7077     | 7077     | `fatih-omen:7077`                      |
| MLflow         | 5000     | 5000     | `fatih-omen:5000`                      |
| Prefect Server | 4200     | 4200     | `fatih-omen:4200`                      |
| Grafana        | 3000     | 3001     | `fatih-omen:3001`                      |
| PgBouncer      | 6432     | 6432     | `fatih-omen:6432`                      |
| Hive Metastore | 9083     | 9083     | `fatih-omen:9083`                      |
| Hive Metastore | 9083     | 9083     | `fatih-omen:9083`                      |
| Nginx (anin)   | 80/443   | 80/443   | `localhost`                            |

---

## Stop Services

### Laptop 1 (fatih-omen)

```bash
docker compose down
```

### Laptop 2 (anin)

```bash
docker compose -f docker-compose.laptop2.yml down
```
