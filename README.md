# Bitcoin Volatility ML

Pipeline ML end-to-end untuk prediksi volatilitas Bitcoin secara real-time, menggabungkan data pasar Binance dengan analisis sentimen Twitter. Dibangun dengan Kafka, Spark Structured Streaming, XGBoost, dan lapisan query terfederasi via Trino.

---

## Arsitektur

```
Binance WebSocket @trade
        │
        ▼
   Kafka (btc_ticker_raw)
        │
   ┌────┴────┐
   ▼         ▼
Spark     Prefect ──► Twitter/X scrape
Streaming            │
   │                 ▼
   ├──► PostgreSQL  FinVADER
   │    (OHLC 1m)     │
   │                  ▼
   │              MinIO (Parquet)
   │                 │
   └──────┬──────────┘
          ▼
      Trino (federated: PG + Hive)
          │
          ▼
    Streamlit Dashboard ◄── Nginx (HTTPS + Basic Auth)

  ════════════════════════════════
  Monitoring & Orchestration
  ════════════════════════════════
  Grafana ─── Telegram Alerts
  Prefect ─── Orchestrasi flow
  MLflow ──── Registry model
  Marquez ─── Data lineage
  Telegraf ── Metrik sistem
```

## Tech Stack

| Kategori | Teknologi |
|---|---|
| **Ingestion** | Binance WebSocket, Kafka, Confluent Schema Registry |
| **Stream Processing** | Apache Spark 3.5.1 (Structured Streaming) |
| **Sentiment Analysis** | Twitter/X scrape, FinVADER |
| **Database** | PostgreSQL 15, PgBouncer (connection pool) |
| **Data Lake** | MinIO (S3-compatible) |
| **Query Engine** | Trino (federated: PostgreSQL + Hive) |
| **Orchestration** | Prefect (server + worker) |
| **ML Framework** | XGBoost, scikit-learn, MLflow |
| **Dashboard** | Streamlit, Plotly |
| **Monitoring** | Grafana, Telegraf, Telegram Bot |
| **Governance** | Marquez (lineage), Great Expectations |
| **Security** | Nginx (HTTPS + Basic Auth), Fernet encryption |
| **Metastore** | Hive Metastore (skema Parquet) |
| **Bahasa** | Python 3.11, PySpark |
| **Container** | Docker, Docker Compose |

---

## Struktur Repository

```
.
├── dashboard/                    # Streamlit dashboard (6 halaman)
│   ├── app.py                    # Main app — Market Overview, Volatility,
│   │                             #   Sentiment, Cross-Source, Lineage, Operations
│   ├── config.py                 # Config koneksi DB & Trino
│   ├── db.py                     # Helper query_pg() & query_trino()
│   ├── data_catalog.py           # Tab Data Catalog (metadata, glossary)
│   ├── data_quality.py           # Tab Data Quality (profiling stats)
│   ├── log_explorer.py           # Tab Log Explorer (app_logs)
│   ├── ml_observability.py       # Tab ML Observability (performa model)
│   ├── system_monitor.py         # Tab System Monitor (container, resource)
│   ├── Dockerfile
│   └── requirements.txt
│
├── ingestion/                    # Ingestion data real-time
│   ├── binance_producer.py       # Binance @trade → Kafka (dengan enkripsi)
│   ├── Dockerfile.producer
│   └── requirements.producer.txt
│
├── processing/                   # Spark Structured Streaming
│   ├── stream_processor.py       # Kafka → tumbling window 1m → OHLC + inference
│   ├── config.py                 # Config Spark & koneksi
│   ├── Dockerfile                # Image Spark worker
│   └── entrypoint.sh
│
├── prefect/                      # Orchestrasi workflow
│   └── flows/
│       ├── sentiment_flow.py     # Twitter scrape → FinVADER → PG (30min)
│       ├── training_flow.py      # Feature engineering → XGBoost → MLflow (harian)
│       ├── system_health_check.py # Kesehatan container & host (60s)
│       ├── data_quality_check.py # Great Expectations + profiling (per jam)
│       ├── model_performance_check.py # Cek degradasi RMSE/MAE (per jam)
│       ├── log_ingester.py       # Agregasi log (5min)
│       └── deploy_all.py         # Daftarkan semua deployment
│
├── ml/                           # Artifak training ML
│   ├── train_model.py            # Script training standalone
│   ├── model_loader.py           # Loader cache model (Spark-side)
│   ├── prepare_data.py           # Persiapan fitur
│   └── mlflow/
│       └── Dockerfile            # Image server MLflow
│
├── governance/                   # Tata kelola data
│   ├── audit_trail.sql           # (placeholder)
│   └── great_expectations/
│       ├── great_expectations.yml
│       └── expectations/         # Suite GE untuk OHLC, sentiment, prediksi
│
├── trino/                        # Trino (query engine federated)
│   ├── config.properties
│   ├── init-schema.sh
│   ├── catalog/
│   │   ├── postgresql.properties
│   │   └── hive.properties
│   └── ddl/
│       └── register_twitter_raw.sql
│
├── security/                     # Keamanan & kontrol akses
│   ├── nginx/
│   │   ├── Dockerfile            # Reverse proxy (HTTPS + Basic Auth)
│   │   ├── nginx.conf
│   │   ├── entrypoint.sh
│   │   ├── .htpasswd             # (kosong, gitignored)
│   │   ├── nginx.crt             # (gitignored)
│   │   └── nginx.key             # (gitignored)
│   └── postgres/
│       ├── init.sql              # Schema lengkap, roles, grants, seed metadata
│       └── create_multiple_db.sh
│
├── hive/                         # Hive Metastore
│   └── hive-site.xml
│
├── monitoring/                   # Observability
│   ├── grafana/
│   │   ├── datasources.yml
│   │   ├── dashboards/
│   │   │   ├── dashboard.yaml
│   │   │   └── pipeline-monitoring.json
│   │   └── provisioning/
│   │       ├── datasources/
│   │       ├── dashboards/
│   │       └── alerting/
│   └── telegraf/
│       ├── Dockerfile
│       ├── telegraf.conf         # Metrik sistem → PostgreSQL
│       └── scripts/
│           └── docker_status.sh
│
├── docker/                       # Dukungan Docker
│   └── hive/
│       ├── Dockerfile
│       └── entrypoint.sh
│
├── pgbouncer/                    # Connection pooler PostgreSQL
│   ├── pgbouncer.ini
│   └── userlist.txt              # Berisi hash MD5 password
│
├── scripts/                      # Script utilitas
│   ├── check_keys.py
│   ├── fetch_historical_btc.py
│   ├── gen_exploration_notebook.py
│   └── seed_dummy_data.py
│
├── notebooks/
│   └── exploration.ipynb         # Notebook EDA (1.5 MB)
│
├── dokumentasi/                  # Dokumentasi proyek (Indonesia)
│   ├── Design-Dashboard.md
│   ├── Laporan.md
│   ├── Requirement Projek.md
│   ├── Resume-Proyek.md
│   └── dashboard-spec.md
│
├── data/                         # Data sampel (placeholder kosong)
│   └── sample_reddit.parquet     # 0 bytes
│
├── docker-compose.yml            # Semua service (Laptop 1 / single machine)
├── docker-compose.laptop2.yml    # Dashboard-only (Laptop 2)
├── .env.example                  # Template environment
├── .env.laptop2.example          # Template env Laptop 2
├── pyproject.toml                # Konfigurasi project UV
├── requirements.txt              # Dependensi Python
├── uv.lock                       # Lock file UV
├── run_pipeline.sh               # CLI: sentiment | train | deploy
├── main.py                       # Entry stub
├── .python-version               # Python 3.11
└── .gitignore
```

---

## Pipeline Data

### 1. Ingestion OHLC Real-Time

```
Binance WebSocket (btcusdt@trade) ──► Kafka (btc_ticker_raw) ──► Spark Streaming
                                                                       │
                                                                  tumbling window 1m
                                                                       │
                                                                  btc_ohlc_1m (PostgreSQL)
                                                                       │
                                                                  XGBoost inference ──► volatility_pred (PG)
                                                                       │
                                                                  Kafka (topic volatility_pred)
```

- **Binance Producer** (`ingestion/binance_producer.py`): Terhubung ke stream `@trade` Binance, mengenkripsi payload dengan Fernet, publish ke Kafka. Exponential backoff dengan 5 retry + alert Telegram jika gagal. Pesan malformed dikirim ke DLQ.
- **Spark Streaming** (`processing/stream_processor.py`): Baca dari Kafka, komputasi tumbling window 1 menit (OHLCV + trade_count + volatility), tulis ke PostgreSQL via PgBouncer dengan pybreaker circuit breaker. Catat lineage ke `pipeline_lineage`.
- **XGBoost Inference Real-Time**: Model dimuat dari MLflow Registry (dicache di `/tmp/xgb_model_cache`). 7 fitur (rolling_vol_5m, price_range_ratio, vol_ratio, compound_score, positive_ratio, tweet_count, minutes_since_sentiment) → predicted_vol_5m. Fallback ke 0 jika model tidak tersedia.

### 2. Analisis Sentimen

```
Twitter/X scrape ──► MinIO (Parquet) ──► FinVADER scoring ──► sentiment_30m (PG)
```

- **Prefect Flow** (`prefect/flows/sentiment_flow.py`): Dijadwalkan setiap 30 menit. Mengumpulkan tweet tentang Bitcoin, menyimpan Parquet mentah di MinIO (`twitter-raw/` bucket), menjalankan skoring sentimen FinVADER, validasi dengan Great Expectations, menulis agregasi window 30 menit ke PostgreSQL.
- **Quality Flags**: `ok` (>5 tweet), `low_sample` (<5), `stale` (forward-filled saat gagal).
- **Circuit Breaker**: Pybreaker — hentikan write setelah 5 kegagalan berturut-turut, reset otomatis setelah 60 detik.

### 3. Training ML

```
Prefect Flow ──► v_ml_features (PostgreSQL view)
                     │
              7 fitur + target_vol_5m
                     │
               XGBoost Regressor
               TimeSeriesSplit 5-fold
               StandardScaler
                     │
                MLflow Registry
                (promote jika MAE < 0.0015)
```

- **Feature View** (`v_ml_features`): JOIN `btc_ohlc_1m` dengan `sentiment_30m` (forward-filled). Menghitung rolling_vol_5m, price_range_ratio, vol_ratio, compound_score, positive_ratio, tweet_count, minutes_since_sentiment. Target: `target_vol_5m` (volatilitas 5 menit ke depan).
- **Training Flow** (`prefect/flows/training_flow.py`): Dijadwalkan setiap hari pukul 23:00 UTC. Ekstrak fitur, train XGBoost (300 trees, max_depth=4, learning_rate=0.05), evaluasi dengan TimeSeriesSplit 5-fold, log ke MLflow. Auto-promote ke Production jika MAE < 0.0015 dan lebih baik dari champion saat ini. Kirim alert Telegram jika gagal.

### 4. Query Terfederasi (Trino)

```
PostgreSQL (btc_ohlc_1m, sentiment_30m, volatility_pred)
        │
        ▼
Trino ──┤
        │
MinIO (tweets Parquet via Hive Metastore)
        │
        ▼
Streamlit Dashboard (cross-source analytics)
```

- Tabel PostgreSQL diquery langsung via katalog `postgresql`.
- File Parquet di MinIO diquery via katalog `hive` (Hive Metastore melacak skema).
- Memungkinkan JOIN across data OHLC real-time + sentimen + tweet mentah dalam satu query SQL.

### 5. Semua Prefect Flows

| Flow | Jadwal | Deskripsi |
|---|---|---|
| `sentiment-pipeline` | Setiap 30 menit | Twitter scrape → FinVADER → PostgreSQL |
| `model-training` | Harian 23:00 UTC | Feature engineering → XGBoost → MLflow |
| `system-health-check` | Setiap 60 detik | Status container, resource host, alert Telegram |
| `data-quality-check` | Setiap 60 menit | Great Expectations + profiling → data_quality_stats |
| `model-performance-check` | Setiap 60 menit | RMSE/MAE produksi (sliding window 6 jam), alert degradasi |
| `log-ingester` | Setiap 5 menit | Agregasi log → app_logs |

---

## Skema Database

### Tabel Inti (PostgreSQL — `btcdb`)

| Tabel | Deskripsi | Sumber |
|---|---|---|
| `btc_ohlc_1m` | Agregasi OHLC per window 1 menit | Spark Streaming (Binance) |
| `sentiment_30m` | Skor sentimen Twitter per window 30 menit | Prefect (FinVADER) |
| `volatility_pred` | Prediksi XGBoost real-time dengan fitur | Spark Streaming (model MLflow) |
| `btc_predictions` | Prediksi vs aktual volatilitas (evaluasi harian) | Prefect (training flow) |
| `pipeline_lineage` | Data lineage: setiap run pipeline (source, target, rows, status) | Semua pipeline |
| `audit_log` | Audit trail otomatis via trigger INSERT/UPDATE/DELETE | Trigger PostgreSQL |

### Tabel Governance

| Tabel | Deskripsi |
|---|---|
| `table_metadata` | Metadata terpusat: deskripsi, owner, sensitivity, frekuensi refresh |
| `business_glossary` | Istilah bisnis yang dipetakan ke tabel/kolom teknis |
| `column_lineage` | Pemetaan kolom source → target dengan transformasi |
| `data_quality_stats` | Statistik profiling: null %, distinct, min/max/mean per kolom |
| `model_performance` | Historical RMSE/MAE per versi model |
| `app_logs` | Log agregasi dari semua service |

### Relasi Utama

```
btc_ohlc_1m.window_start ──┐
                            ├──► v_ml_features (training view)
sentiment_30m.window_start ─┘
       │                              ┌──► btc_predictions (evaluasi harian)
       └──► volatility_pred ──────────┤
                            XGBoost    └──► Kafka (topic pred real-time)
```

---

## Dashboard

6 halaman Streamlit yang disajikan di belakang Nginx (HTTPS + Basic Auth):

| Halaman | Deskripsi |
|---|---|
| **Market Overview** | Candlestick chart live, harga terbaru, volume, perubahan 24 jam |
| **Volatility Analytics** | Prediksi vs aktual volatilitas, feature importances, tren MAE model |
| **Sentiment Analytics** | Timeline compound score, rasio positif/negatif, volume tweet |
| **Cross-Source Analytics** | Query Trino federated: JOIN OHLC + sentimen + tweet mentah |
| **Data Lineage** | Tabel pipeline_lineage, riwayat run dengan status kualitas |
| **Operations** | Data Catalog (metadata, glossary), Data Quality (profiling), System Monitor (container, CPU/mem/disk), Log Explorer, ML Observability |

Grafana di port 3001 menyediakan monitoring tambahan dengan dashboard bawaan dan alerting Telegram (4 aturan alert).

---

## Referensi Service

| Service | Container | Host Port | Kredensial |
|---|---|---|---|
| Streamlit Dashboard | `dashboard` | `8501:8501` | (via Nginx) |
| Nginx (HTTPS + Auth) | `nginx` | `8443:443` | `kelompok4_ipbd` / `k4ipbd_nginx_2026` |
| PostgreSQL | `postgres` | `5434:5432` | `kelompok4_ipbd` / `k4ipbd_postgres_2026` |
| PgBouncer | `pgbouncer` | `6432:6432` | (pooled PG, kredensial sama) |
| Trino | `trino` | `8082:8080` | user: `kelompok4_ipbd` |
| Kafka | `kafka` | `9092:9092` | — |
| Kafka UI | `kafka-ui` | `8083:8080` | — |
| MinIO S3 API | `minio` | `9002:9000` | `minioadmin` / `k4ipbd_minio_2026` |
| MinIO Console | `minio` | `9003:9001` | sama |
| Spark Master | `spark-master` | `8081:8080` / `7077:7077` | — |
| MLflow | `mlflow` | `5001:5000` | — |
| Prefect Server | `prefect-server` | `4201:4200` | — |
| Grafana | `grafana` | `3001:3000` | `admin` / `k4ipbd_grafana_2026` |
| Marquez API | `marquez-api` | `5002:5000` | — |
| Marquez Web | `marquez-web` | `3002:3000` | — |
| Hive Metastore | `hive-metastore` | `9083:9083` | — |
| Zookeeper | `zookeeper` | `2181:2181` | — |

---

## Keamanan & Tata Kelola

### Enkripsi In-Transit
- Pesan Kafka dienkripsi dengan **Fernet symmetric encryption** sebelum dipublish (`ingestion/binance_producer.py`).
- Kunci dekripsi dari env `ENCRYPTION_KEY`; dikonsumsi oleh Spark Streaming untuk inference.

### Kontrol Akses
- **Nginx reverse proxy**: HTTPS (self-signed cert) + HTTP Basic Auth untuk Streamlit dashboard.
- **Role PostgreSQL**: `kelompok4_ipbd` (admin penuh), `dashboard_reader` (read-only untuk dashboard), `marquez` (owner DB lineage).
- **PgBouncer**: Transaction pooling dengan autentikasi `userlist.txt` (hash MD5).

### Tata Kelola Data
- **Audit Trigger**: Pencatatan INSERT/UPDATE/DELETE otomatis ke tabel `audit_log` untuk semua tabel inti.
- **Data Lineage**: Setiap run pipeline mencatat source, target, rows_processed, quality_status ke `pipeline_lineage`. Marquez mengumpulkan event OpenLineage untuk visualisasi lineage.
- **Great Expectations**: 3 suite validasi (OHLC, sentimen, prediksi) dijalankan setiap jam via Prefect.
- **Data Catalog**: `table_metadata` terpusat (owner, sensitivity, frekuensi refresh), `business_glossary`, `column_lineage` (pemetaan source → target dengan transformasi).
- **Data Quality**: Profiling setiap jam (null %, distinct, min/max/mean), disimpan di `data_quality_stats`.

### Monitoring Sistem
- **Telegraf**: Mengumpulkan metrik CPU, memory, disk, dan container Docker setiap 15 detik → PostgreSQL.
- **Grafana**: Dashboard bawaan + 4 aturan alert (notifikasi Telegram).
- **Prefect health check**: Setiap 60 detik — status container, resource host, alert anomali.
- **Model performance check**: Setiap jam — menghitung RMSE/MAE produksi, alert jika degradasi > 50%.

---

## Verifikasi

### Cek Aliran Data (PostgreSQL)

```sql
-- Data OHLC real-time
SELECT COUNT(*) AS rows, MAX(window_start) AS latest
FROM btc_ohlc_1m;

-- Data sentimen
SELECT COUNT(*) AS rows, MAX(window_start) AS latest
FROM sentiment_30m;

-- Prediksi
SELECT COUNT(*) AS rows, MAX(window_start) AS latest
FROM volatility_pred;

-- Pipeline lineage
SELECT pipeline_name, quality_status, COUNT(*)
FROM pipeline_lineage
GROUP BY 1, 2
ORDER BY 1;
```

### Query Terfederasi (Trino)

```sql
-- JOIN PostgreSQL OHLC + MinIO tweets via Trino
SELECT o.window_start, o.close, t.compound
FROM postgresql.public.btc_ohlc_1m o
LEFT JOIN hive.twitter_raw.tweets t
  ON date_trunc('minute', o.window_start) = date_trunc('minute', t.created_at)
LIMIT 10;
```

### Monitoring
- Grafana: `http://host:3001` — cek alert rules dan pipeline dashboard.
- Prefect: `http://host:4201` — lihat flow runs, task logs, jadwal deployment.
- MLflow: `http://host:5001` — model registry, experiment tracking, feature importance.
- Marquez: `http://host:3002` — grafik lineage data visual.
- Kafka UI: `http://host:8083` — browser topic, consumer groups.
