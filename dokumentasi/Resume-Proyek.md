# Bitcoin Volatility Prediction — End-to-End Big Data Pipeline

**Kelompok 4 — IPBD 2026**

---

## 1. Deskripsi Proyek

Proyek ini membangun pipeline big data end-to-end untuk memprediksi **volatilitas Bitcoin jangka pendek (5 menit ke depan)** dengan mengintegrasikan data harga real-time dari **Binance WebSocket** dan data sentimen media sosial dari **Twitter/X** menggunakan machine learning (XGBoost Regressor). Seluruh sistem berjalan di atas **Docker Compose** (24 layanan) dengan arsitektur stream dan batch processing.

---

## 2. Tech Stack

### Infrastruktur
| Kategori | Teknologi |
|---|---|
| Containerization | Docker, Docker Compose v2 |
| Networking | Tailscale (split-machine deployment) |
| Load Balancer | Nginx (HTTPS + Basic Auth) |

### Ingestion & Messaging
| Kategori | Teknologi |
|---|---|
| Message Broker | Apache Kafka (Confluent 7.6.0) |
| Stream Ingestion | Binance WebSocket (`btcusdt@trade`) |
| Batch Ingestion | tweet-harvest (Node.js + Playwright) |

### Processing
| Kategori | Teknologi |
|---|---|
| Stream Processing | Apache Spark 3.5.1 Structured Streaming |
| Batch Orchestration | Prefect 2.x (6 flow) |
| Query Engine | Trino 438 (federated PostgreSQL + MinIO) |
| Connection Pooler | PgBouncer 1.24.1 |

### Storage
| Kategori | Teknologi |
|---|---|
| Relational DB | PostgreSQL 15 (6 database) |
| Data Lake | MinIO (S3-compatible) — 4 bucket |
| Schema Registry | Hive Metastore 3.0.0 |

### Machine Learning
| Kategori | Teknologi |
|---|---|
| Model | XGBoost Regressor (7 fitur, 1 target) |
| Experiment Tracking | MLflow 2.14.3 |
| Model Registry | MLflow Registry (Production alias) |
| Validation | Great Expectations (3 expectation suites) |
| Libraries | Scikit-learn, joblib, pandas |

### Visualization & Monitoring
| Kategori | Teknologi |
|---|---|
| Dashboard | Streamlit 1.31.0 + Plotly 5.22.0 |
| Monitoring | Grafana 10.3.3 + Telegraf 1.30 |
| Alerting | Telegram Bot |
| Lineage | OpenLineage + Marquez 0.51.1 |

### Security
| Kategori | Teknologi |
|---|---|
| Encryption | Fernet (AES-128-CBC + HMAC) — Kafka |
| Access Control | PostgreSQL RBAC (dashboard_reader) |
| Secrets | `.env` environment variables |

---

## 3. Arsitektur Pipeline

```
Binance WebSocket ──► Kafka ──► Spark Streaming ──► PostgreSQL (OHLC + predictions)
                                                          │
Twitter/X ──► Prefect ──► MinIO (Parquet) ───────────────┤
                                                          │
                                    Trino (federated: PG + MinIO Parquet)
                                         │
                                    Streamlit Dashboard
                                         │
                                    Nginx (HTTPS + Basic Auth)
```

### Detail Pipeline

#### A. Stream Layer (Real-time)

| Pipeline | Source | Processing | Output | Schedule |
|---|---|---|---|---|
| **Binance Ingestion** | Binance WebSocket `btcusdt@trade` | Parse tick, Fernet encrypt | Kafka `btc_ticker_raw` | Real-time |
| **Spark Streaming** | Kafka `btc_ticker_raw` | Decrypt → Tumbling 1-min OHLC → Feature engineering → XGBoost inference (load from MLflow Registry) | `btc_ohlc_1m`, `volatility_pred`, Kafka `volatility_pred` | Trigger 30s |

#### B. Batch Layer (Scheduled)

| Pipeline | Source | Processing | Output | Schedule | File |
|---|---|---|---|---|---|
| **Sentiment** | tweet-harvest → Twitter/X scrape | FinVADER scoring → 30-min aggregation → GE validation | MinIO `twitter-raw/tweets/*.parquet`, `sentiment_30m` | Setiap 30 menit | `prefect/flows/sentiment_flow.py` |
| **Model Training** | PostgreSQL `v_ml_features` | View JOIN OHLC + sentimen → XGBoost Regressor (TimeSeriesSplit 5-fold) → MLflow Registry | Model di MLflow, promote Production jika MAE < 0.0015 | Harian (23:00 UTC) | `prefect/flows/training_flow.py` |
| **System Health Check** | Docker socket + psutil | Container status, CPU/mem/disk | `pipeline_lineage`, Telegram alert | Setiap 60 detik | `prefect/flows/system_health_check.py` |
| **Data Quality Check** | PostgreSQL (4 tabel) | Profiling null%, distinct, min/max/mean + 3 GE suites | `data_quality_stats`, Telegram alert | Setiap 1 jam | `prefect/flows/data_quality_check.py` |
| **Model Performance Check** | `btc_predictions` | Backfill actual vol, compute RMSE/MAE sliding 6h | `model_performance`, alert jika degradasi > 200% | Setiap 1 jam | `prefect/flows/model_performance_check.py` |
| **Log Ingestion** | `pipeline_lineage` + `audit_log` | Aggregate by service/level, cleanup > 30 hari | `app_logs`, Telegram alert | Setiap 5 menit | `prefect/flows/log_ingester.py` |

---

## 4. Dataset & Skema Database

### PostgreSQL (btcdb) — 11 Tabel

| Tabel | Deskripsi | Sumber |
|---|---|---|
| `btc_ohlc_1m` | OHLC agregasi 1 menit + volatilitas | Spark Streaming |
| `sentiment_30m` | Skor sentimen Twitter agregasi 30 menit | Prefect Sentiment Flow |
| `volatility_pred` | Prediksi volatilitas 5 menit (XGBoost) + fitur + latency | Spark Streaming Inference |
| `btc_predictions` | Prediksi vs aktual volatilitas untuk evaluasi | Prefect Model Perf Check |
| `pipeline_lineage` | Metadata setiap pipeline run (source, target, rows, status) | Semua Prefect Flow |
| `audit_log` | Audit trail otomatis (trigger INSERT/UPDATE/DELETE) | PostgreSQL Trigger |
| `data_quality_stats` | Statistik profiling per tabel per kolom | Prefect DQ Check |
| `table_metadata` | Metadata tabel (deskripsi, owner, sensitivity, refresh) | Init SQL |
| `business_glossary` | Istilah bisnis mapped ke tabel/kolom teknis | Init SQL |
| `column_lineage` | Trace source → target per kolom + transformasi | Init SQL |
| `model_performance` | Evaluasi performa model (RMSE, MAE, n_predictions) | Prefect Model Perf Check |

### View
- `v_ml_features`: JOIN `btc_ohlc_1m` + `sentiment_30m` (forward-fill) untuk training

### MinIO Buckets
- `twitter-raw/` — Tweet Parquet mentah
- `mlflow-artifacts/` — Model artifacts
- `checkpoints/` — Spark Streaming checkpoints
- `hive-warehouse/` — Hive Metastore warehouse

### Sistem Monitoring (Telegraf → PostgreSQL)
- `cpu`, `mem`, `disk`, `docker`, `docker_container_cpu`, `docker_container_mem` — auto-created tables

---

## 5. Model Machine Learning

| Aspek | Detail |
|---|---|
| **Model** | XGBoost Regressor |
| **Registered Name** | `btc_volatility_xgb` (MLflow) |
| **Experiment** | `btc_volatility_prediction` |

### 7 Fitur Input

| Fitur | Sumber | Keterangan |
|---|---|---|
| `rolling_vol_5m` | OHLC | Std dev return 5 menit rolling |
| `price_range_ratio` | OHLC | (high - low) / close |
| `vol_ratio` | OHLC | volume / avg_volume_10min |
| `compound_score` | Sentimen | FinVADER compound score [-1, +1] |
| `positive_ratio` | Sentimen | Proporsi tweet positif |
| `tweet_count` | Sentimen | Jumlah tweet dalam window |
| `minutes_since_sentiment` | Sentimen | Staleness data sentimen |

### Target
- `target_vol_5m`: Std dev return 5 menit ke depan

### Hyperparameter
- `n_estimators=300`, `max_depth=4`, `learning_rate=0.05`
- `subsample=0.8`, `colsample_bytree=0.8`, `min_child_weight=5`
- `tree_method="hist"`, `early_stopping_rounds=20`
- Cross-validation: TimeSeriesSplit 5-fold
- Auto-promote Production jika MAE < 0.0015

### Inference
- Model + scaler dimuat dari MLflow Registry (Production alias) setiap startup
- Background thread refresh setiap 5 menit
- Fallback ke local cache `/tmp/xgb_model_cache/`

---

## 6. Dashboard (Streamlit)

**URL:** `https://<host>:8443` (HTTPS + Basic Auth via Nginx)

6 halaman utama disajikan dalam sidebar radio menu:

| Halaman | Konten Utama |
|---|---|
| **Summary** | 4 KPI card (BTC price, sentiment, predictions, system health), mini chart BTC price + CPU gauge + model perf |
| **Market Overview** | 6 KPI card, candlestick chart 24h, volume bar per jam, volatility timeseries, price histogram |
| **Volatility Analytics** | Predicted volatility timeseries, feature importance, scatter volume vs vol, box plot vol per jam, predicted-vs-actual, residual histogram, model performance history |
| **Sentiment Analytics** | Sentiment timeseries, donut ratio, tweet volume, weighted vs unweighted comparison, Trino federated query (PG JOIN MinIO) + korelasi sentimen-harga + SQL explorer |
| **Pipeline Operations** | Pipeline lineage timeline Gantt, latency timeseries, audit log table + donut distribusi operasi |
| **Operations** (4 sub-tab) | **System Health:** container status table, CPU/mem/disk gauges + timeseries, container resource trends. **Data Quality:** completeness KPI, null heatmap, data freshness, column profiling table, validation history. **Data Catalog:** lineage Sankey, table browser + metadata + column info, column-level lineage, business glossary. **Logs:** log filter + table, level distribution, error timeseries, pipeline error summary, recent alerts |

---

## 7. Monitoring & Alerting

### Grafana (9 Alert Rules)
**Pipeline alerts (4):** Spark streaming lag, sentiment pipeline failure, Prefect flow failure, model performance degradation
**System alerts (5):** CPU > 80%, Memory > 85%, Disk > 85%, Container exited, PostgreSQL connection down

### Alert Destination
- **Telegram Bot** untuk semua alert

### Telegraf
- Collection interval: 15 detik
- Metrics: CPU, memory, disk, Docker container stats → PostgreSQL

---

## 8. Security

| Layer | Mekanisme |
|---|---|
| **Kafka** | Fernet AES-128-CBC + HMAC enkripsi in-transit |
| **PostgreSQL** | RBAC: `btcadmin` (read-write), `dashboard_reader` (read-only) |
| **Nginx** | HTTPS (self-signed TLS) + Basic Auth |
| **Credentials** | Semua kredensial di `.env`, tidak hardcode |

---

## 9. Data Governance

| Aspek | Implementasi |
|---|---|
| **Data Quality** | Great Expectations 3 suites + Prefect profiling hourly |
| **Audit Trail** | Trigger PostgreSQL otomatis pada INSERT/UPDATE/DELETE |
| **Data Lineage** | `column_lineage` table + OpenLineage/Marquez + Sankey diagram di dashboard |
| **Metadata** | `table_metadata` + `business_glossary` tabel + Data Catalog page |
| **Data Freshness** | Monitoring umur data per tabel di dashboard |

---

## 10. Struktur Direktori

```
bitcoin-volatility-ml/
├── ingestion/                  # Binance WebSocket → Kafka producer
├── processing/                 # Spark Streaming: OHLC + inference
├── prefect/                    # Prefect flows (6 pipeline)
│   └── flows/                  # sentiment, training, health, DQ, model-perf, log
├── ml/                         # ML model helpers
│   └── mlflow/                 # MLflow Dockerfile
├── dashboard/                  # Streamlit app (6 pages + modules)
├── trino/                      # Trino config + catalog connectors
├── hive/                       # Hive Metastore config
├── docker/hive/                # Custom Hive Dockerfile
├── monitoring/                 # Grafana dashboards, alerts, Telegraf
│   ├── grafana/                # Dashboards, datasources, alert rules
│   └── telegraf/               # Telegraf config
├── security/                   # Nginx config, PG init.sql, crypto
├── pgbouncer/                  # PgBouncer config
├── governance/                 # Great Expectations config + suites
├── scripts/                    # seed data, fetch historical, check_keys
├── notebooks/                  # Jupyter exploration
├── data/                       # Sample data
├── dokumentasi/                # Laporan & requirement
├── docker-compose.yml          # 24 service definition
├── docker-compose.laptop2.yml  # Split-machine deployment
├── .env                        # Environment variables (active)
└── .env.example                # Env template
```

---

## 11. Cara Menjalankan

```bash
# 1. Clone & setup env
cp .env.example .env
# edit .env dengan credentials yang sesuai

# 2. Start semua service
docker compose up -d

# 3. Cek status semua container
docker compose ps

# 4. Seed dummy data (jika tidak ada data real)
docker exec -it prefect-worker python scripts/seed_dummy_data.py

# 5. Akses dashboard
# https://localhost:8443  (HTTPS)
# http://localhost:8080   (HTTP redirect)

# 6. Akses service lainnya
# MLflow        → http://localhost:5001
# Grafana       → http://localhost:3001
# MinIO Console → http://localhost:9003
# Spark Master  → http://localhost:8081
# Trino         → http://localhost:8082
# Prefect       → http://localhost:4201
# Kafka-UI      → http://localhost:8083
# Marquez       → http://localhost:3002
```

## 12. Roadmap (Fase 2–4)

- K-Means clustering — market regime detection (Stabil/Sedang/Tinggi)
- Model ensemble (XGBoost + LightGBM + LSTM)
- Multi-coin support (ETH, SOL)
- Anomaly detection untuk spike volatilitas
- Auto-scaling Spark cluster
