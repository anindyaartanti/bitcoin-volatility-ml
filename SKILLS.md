# SKILLS.md — Bitcoin Volatility ML
## Pemetaan Implementasi terhadap Rubrik Penilaian TBP

---

## 1. Perancangan Arsitektur Pipeline (10)

**Alur end-to-end:**

```
[Data Source]
  Binance WebSocket (btcusdt@trade)
  Twitter/X via tweet-harvest

       ↓ stream                    ↓ batch (setiap 30 menit)

[Message Broker]             [Object Storage]
  Kafka (btc_ticker_raw)       MinIO: twitter-raw/tweets/YYYY/MM/DD/HH_MM.parquet

       ↓ Spark Structured Streaming   ↓ FinVADER Scoring

[Processing]
  stream_processor.py → 1-menit OHLC window aggregation
  sentiment_flow.py   → compound/positive/negative/neutral score per 30 menit

       ↓                         ↓

[Storage]
  PostgreSQL (btcdb)
    ├── btc_ohlc_1m      (OHLC Bitcoin per menit)
    ├── sentiment_30m    (skor sentimen per 30 menit)
    ├── pipeline_lineage (data lineage setiap run)
    └── audit_log        (audit trail INSERT/UPDATE/DELETE)

       ↓ JOIN fitur OHLC + sentimen

[ML Training]
  MLflow tracking → XGBoost → model btc-volatility-xgboost (MinIO: mlflow-artifacts)

       ↓

[Query Engine]         [Serving]           [Monitoring]
  Trino                 Streamlit            Grafana + Telegram
  (PostgreSQL + MinIO)  (dashboard)          (alerting)
```

**Komponen infrastruktur:**

| Layer | Teknologi | File Konfigurasi |
|---|---|---|
| Ingestion stream | Binance WebSocket + Kafka | `ingestion/binance_producer.py` |
| Ingestion batch | tweet-harvest + MinIO | `prefect/flows/sentiment_flow.py` |
| Stream processing | Spark Structured Streaming | `processing/stream_processor.py` |
| Batch processing | FinVADER + Prefect | `prefect/flows/sentiment_flow.py` |
| Storage | PostgreSQL + MinIO | `security/postgres/init.sql` |
| Connection pooling | PgBouncer | `pgbouncer/pgbouncer.ini` |
| Query engine | Trino | `trino/catalog/postgresql.properties` |
| ML tracking | MLflow | `prefect/flows/model_training.py` |
| Orkestrasi | Prefect | `prefect/flows/deploy_all.py` |
| Monitoring | Grafana | `monitoring/grafana/` |
| Alerting | Telegram Bot | (env: TELEGRAM_BOT_TOKEN) |
| Keamanan | Nginx + PgBouncer auth | `security/nginx/nginx.conf` |
| Kontainerisasi | Docker Compose | `docker-compose.yml` |

---

## 2. Batch Processing (10)

**Implementasi:** `prefect/flows/sentiment_flow.py` — flow `sentiment_pipeline`

**Alur batch:**
1. `harvest_tweets()` — scrape Twitter per keyword (bitcoin, BTC, crypto), output CSV → Parquet
2. Upload Parquet ke MinIO bucket `twitter-raw` dengan path `tweets/YYYY/MM/DD/HH_MM.parquet`
3. `score_and_store()` — baca Parquet dari MinIO, scoring FinVADER per baris
4. Agregasi per window 30 menit → UPSERT ke `sentiment_30m`

**Jadwal:** setiap 30 menit via Prefect `IntervalSchedule(interval=1800)`

**Perbedaan dengan stream:**
- Batch tidak real-time — data dikumpulkan dulu, diproses sekaligus
- Batch bisa diulang (idempotent via `ON CONFLICT DO UPDATE`)
- Batch menyimpan raw data dulu ke MinIO sebelum diproses (lake-first pattern)

**Dependency:** `prefect/requirements.txt` — finvader, pandas, pyarrow, s3fs, minio

---

## 3. Stream Processing (15)

**Implementasi:** `processing/stream_processor.py` — Spark Structured Streaming

**Alur stream:**
1. Subscribe Kafka topic `btc_ticker_raw` (3 partisi)
2. Parse JSON: `{event_time, symbol, trade_id, price, quantity, is_buyer_mm}`
3. Filter null price/quantity
4. **Tumbling window 1 menit** — agregasi:
   - `open` = first price
   - `high` = max price
   - `low` = min price
   - `close` = last price
   - `volume` = sum(quantity)
   - `trade_count` = count(*)
   - `volatility` = custom UDF (std harga dalam window)
5. UPSERT ke PostgreSQL `btc_ohlc_1m` via PgBouncer (JDBC)
6. Log lineage ke `pipeline_lineage` setiap micro-batch

**Fault tolerance:**
- Circuit breaker (pybreaker): 5 gagal berturut → open 60 detik, kirim Telegram alert
- Checkpoint di MinIO `s3a://checkpoints/spark-streaming/` untuk recovery
- DLQ topic `btc_ticker_dlq` untuk pesan malformed
- Binance producer: exponential backoff 5x retry, DNS-over-HTTPS bypass

**Perbedaan dengan batch:**
- Latensi milidetik (sub-second dari Binance sampai PostgreSQL)
- Tidak simpan raw data, langsung agregasi
- State dijaga Spark Structured Streaming (watermark, checkpoint)

---

## 4. Integrasi dengan Machine Learning (15)

**Implementasi:** `prefect/flows/model_training.py`

**Alur integrasi:**
1. Cek ketersediaan data: `btc_ohlc_1m` > 1000 rows, `sentiment_30m` > 24 rows
2. **Feature engineering** via SQL JOIN antara OHLC dan sentimen:
   - Fitur OHLC: `rolling_vol_5m`, `price_range_ratio`, `vol_ratio`
   - Fitur sentimen: `avg_sentiment`, `mention_count`, `positive_ratio`
   - Target: `target_vol_5m` (forward 5-menit volatility)
3. Training XGBoost Regressor:
   - 200 estimators, max_depth=6, learning_rate=0.05, subsample=0.8
   - Split 80/20 tanpa shuffle (time series)
4. **MLflow tracking:** params, metrics (RMSE, MAE), feature importances, model artifact
5. Register model `btc-volatility-xgboost` ke MLflow Model Registry
6. Promote ke stage **Production**

**Integrasi stream → ML:**
- `stream_processor.py` load model Production dari MLflow saat startup
- Cache lokal `/tmp/xgb_model_cache/model.pkl` sebagai fallback
- Model siap digunakan untuk inference real-time

**Jadwal training:** setiap Senin jam 02:00 UTC (weekly retrain)

---

## 5. Visualisasi & Dashboard (10)

**Grafana** — `monitoring/grafana/dashboards/`
- `pipeline-monitoring.json`: row count per tabel, execution time, lineage status
- `node-metrics.json`: resource utilization (CPU, memory)
- Datasource: PostgreSQL (btcdb) via provisioning otomatis

**Streamlit** — `dashboard/` *(in progress)*
- Query via Trino (unified access PostgreSQL + MinIO Parquet)
- Target: candlestick OHLC, sentiment time series, volatility forecast

**Trino sebagai query engine:**
- Catalog `postgresql` → query `btc_ohlc_1m`, `sentiment_30m`
- Catalog `hive` → query Parquet di MinIO (raw tweets)
- Memungkinkan satu query JOIN antara PostgreSQL dan MinIO

---

## 6. Monitoring & Logging (10)

**Pipeline lineage** — tabel `pipeline_lineage` di PostgreSQL:
```sql
run_id, pipeline_name, source, target_table,
rows_processed, rows_rejected, quality_status,
started_at, finished_at, params (JSONB)
```
Diisi setiap run oleh `sentiment_flow.py` dan `stream_processor.py`.

**Audit log** — tabel `audit_log` di PostgreSQL:
- Trigger otomatis di `btc_ohlc_1m`, `sentiment_30m`, `pipeline_lineage`
- Capture INSERT/UPDATE/DELETE: old_data, new_data, changed_by, changed_at

**Application logging:**
- `binance_producer.py`: Python logging ke stdout (level INFO/WARNING/ERROR)
- `stream_processor.py`: Spark logging + custom Python logger `stream_processor`
- `sentiment_flow.py`: Prefect structured logging per task

**Grafana dashboard:** visualisasi metrics pipeline dari PostgreSQL secara real-time

---

## 7. Alerting System (10)

**Telegram Bot** — dipakai di 2 titik:

**A. Binance Producer** (`ingestion/binance_producer.py`):
- Alert ketika WebSocket disconnect dan semua retry habis (5x exponential backoff)
- Alert setiap kali pesan dikirim ke DLQ (data malformed)

**B. Sentiment Flow** (`prefect/flows/sentiment_flow.py`):
- Alert ketika circuit breaker ke PostgreSQL OPEN (5 gagal berturut)
- Alert ketika flow `sentiment_pipeline` gagal setelah 3x retry:
  ```
  [sentiment_pipeline] FAILED untuk window 2026-06-13 01:30:00: <error>
  ```
- Forward-fill otomatis: baris terakhir di-copy ke window gagal dengan `data_quality='stale'`

**Konfigurasi:** env `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` di `.env`

---

## 8. Keamanan Data (10)

**Authentication & Authorization:**
- PostgreSQL: role `btcadmin` (write) + `dashboard_reader` (read-only SELECT)
- PgBouncer: auth via `userlist.txt` (md5), transaction pooling — koneksi tidak langsung ke PostgreSQL
- MinIO: access key + secret key, tidak expose tanpa kredensial
- Grafana: admin login (user/password via env)

**Network isolation:**
- Semua service dalam Docker network `bigdata_net` (bridge, tidak expose ke host kecuali port yang diperlukan)
- PostgreSQL hanya expose port 5434 ke host untuk debugging

**Nginx reverse proxy** (`security/nginx/`):
- SSL/TLS termination (self-signed cert: `nginx.crt`, `nginx.key`)
- Basic auth `.htpasswd` untuk akses dashboard
- Konfigurasi siap pakai, dapat diaktifkan di `docker-compose.yml`

**Data protection:**
- Kredensial hanya dari environment variable, tidak hardcode di kode
- `.env` tidak di-commit (ada `.env.example` sebagai template)
- PgBouncer `POOL_MODE=transaction` mencegah session leakage

---

## 9. Governance Big Data (5)

**Data Quality** — Great Expectations inline di `sentiment_flow.py`:
```python
# Validasi sebelum simpan ke PostgreSQL
- Tidak ada null di kolom teks dan compound score
- compound_score harus dalam range [-1, 1]
- Jika gagal → raise ValueError, tidak ada data korup masuk DB
```

**Data Quality flag** — kolom `data_quality` di `sentiment_30m`:
- `ok` — data normal (≥5 tweets)
- `low_sample` — kurang dari 5 tweets di window tersebut
- `stale` — window gagal, diisi forward-fill dari data terakhir

**Metadata & Lineage:**
- `pipeline_lineage`: setiap run tercatat lengkap (source, target, rows, params JSONB, status)
- `audit_log`: setiap perubahan data tercatat otomatis via PostgreSQL trigger
- MinIO path: `tweets/YYYY/MM/DD/HH_MM.parquet` — terstruktur untuk partisi waktu

**Schema governance:**
- Semua tabel didefinisikan di `security/postgres/init.sql` dengan constraints ketat
- CHECK constraints: harga > 0, high ≥ low, sentiment BETWEEN -1 AND 1
- UNIQUE constraints mencegah duplikasi window

---

## 10. Dokumentasi & Reproducibility (5)

**Cara menjalankan ulang dari nol:**

```bash
# 1. Clone repo dan setup env
cp .env.example .env
# Edit .env: isi TWITTER_AUTH_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

# 2. Build dan jalankan semua service
docker compose up -d --build

# 3. Tunggu semua healthy (~3-5 menit), cek status
docker compose ps

# 4. Verifikasi Binance masuk PostgreSQL (otomatis via streaming)
docker exec -it postgres psql -U btcadmin -d btcdb \
  -c "SELECT COUNT(*) FROM btc_ohlc_1m;"

# 5. Trigger sentiment pipeline manual (atau tunggu 30 menit)
# Buka http://localhost:4200 → Deployments → sentiment-pipeline → Quick Run

# 6. Verifikasi sentimen masuk PostgreSQL
docker exec -it postgres psql -U btcadmin -d btcdb \
  -c "SELECT window_start, compound_score, tweet_count FROM sentiment_30m ORDER BY window_start DESC LIMIT 5;"

# 7. Lihat dashboard
# Grafana:  http://localhost:3000  (admin/grafana123)
# MLflow:   http://localhost:5000
# Prefect:  http://localhost:4200
# MinIO:    http://localhost:9001  (minioadmin/minioadmin123)
# Trino:    http://localhost:8082
# Spark:    http://localhost:8081
```

**Struktur kode:**

```
bitcoin-volatility-ml/
├── ingestion/          # Binance WebSocket producer
├── processing/         # Spark Streaming (OHLC aggregation)
├── prefect/
│   ├── flows/          # sentiment_flow.py, model_training.py, deploy_all.py
│   ├── Dockerfile
│   └── requirements.txt
├── security/
│   ├── postgres/       # init.sql (schema), create_multiple_db.sh
│   └── nginx/          # SSL cert, basic auth
├── monitoring/grafana/ # Dashboard JSON, provisioning
├── trino/catalog/      # postgresql.properties, hive.properties
├── pgbouncer/          # pgbouncer.ini, userlist.txt
├── dashboard/          # Streamlit app
├── docker-compose.yml
└── .env.example
```

**Port yang digunakan:**

| Service | Port | Fungsi |
|---|---|---|
| PostgreSQL | 5434 | Database utama |
| MinIO API | 9000 | S3-compatible storage |
| MinIO Console | 9001 | Web UI object storage |
| Kafka | 9092 | Message broker |
| Spark Master UI | 8081 | Monitoring Spark jobs |
| MLflow | 5000 | ML tracking + model registry |
| Prefect | 4200 | Workflow orchestration UI |
| Trino | 8082 | Query engine |
| Grafana | 3000 | Dashboard monitoring |
| PgBouncer | 6432 | Connection pooler |
