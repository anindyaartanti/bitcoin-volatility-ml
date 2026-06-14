# SKILLS.md — Bitcoin Volatility ML
## Pemetaan Implementasi terhadap Rubrik Penilaian TBP

---

## 1. Perancangan Arsitektur Pipeline (10)

**Alur end-to-end:**

```
[Data Source]
  Binance WebSocket (btcusdt@trade)          Twitter/X via tweet-harvest
          |                                           |
          | stream real-time                          | batch setiap 30 menit
          ▼                                           ▼
  [Message Broker]                         [Object Storage]
  Kafka topic: btc_ticker_raw              MinIO bucket: twitter-raw
  (3 partisi, retensi 24 jam)              tweets/YYYY/MM/DD/HH_MM.parquet
          |                                           |
          | Spark Structured Streaming                | FinVADER scoring
          ▼                                           ▼
  [Processing]
  stream_processor.py                      sentiment_flow.py
  → tumbling window 1 menit                → compound/pos/neg/neu score
  → OHLC + volatility UDF                  → agregasi per window 30 menit
          |                                           |
          ▼                                           ▼
  [Storage — PostgreSQL btcdb]
  btc_ohlc_1m         sentiment_30m        pipeline_lineage    audit_log
  (OHLC per menit)    (sentimen 30 menit)  (data lineage)      (audit trail)
          |                   |
          └────────┬──────────┘
                   | JOIN fitur OHLC + sentimen
                   ▼
  [ML Training — Prefect weekly]
  model_training.py → XGBoost Regressor
  MLflow tracking → model btc-volatility-xgboost
  artifact disimpan di MinIO: mlflow-artifacts
                   |
                   ▼
  [Query Engine]         [Serving]           [Monitoring & Alerting]
  Trino                  Streamlit            Grafana (dashboard)
  catalog postgresql     (dashboard)          Telegram Bot (alert)
  catalog hive (MinIO)
```

**Komponen infrastruktur:**

| Layer | Teknologi | File Utama |
|---|---|---|
| Ingestion stream | Binance WebSocket → Kafka | `ingestion/binance_producer.py` |
| Ingestion batch | tweet-harvest → MinIO | `prefect/flows/sentiment_flow.py` |
| Stream processing | Spark Structured Streaming | `processing/stream_processor.py` |
| Batch processing | FinVADER + Prefect | `prefect/flows/sentiment_flow.py` |
| Storage relasional | PostgreSQL + PgBouncer | `security/postgres/init.sql` |
| Object storage | MinIO (S3-compatible) | `docker-compose.yml` |
| Query engine | Trino | `trino/catalog/` |
| ML tracking & registry | MLflow | `prefect/flows/model_training.py` |
| Orkestrasi | Prefect | `prefect/flows/deploy_all.py` |
| Monitoring | Grafana | `monitoring/grafana/` |
| Alerting | Telegram Bot | env: `TELEGRAM_BOT_TOKEN` |
| Keamanan | Nginx SSL + PgBouncer auth | `security/nginx/` |
| Kontainerisasi | Docker Compose | `docker-compose.yml` |

---

## 2. Batch Processing (10)

**Implementasi:** `prefect/flows/sentiment_flow.py` — flow `sentiment_pipeline`

**Alur batch:**

1. **`harvest_tweets()`** — jalankan `npx tweet-harvest` untuk 3 keyword: `bitcoin`, `BTC`, `crypto`
   - Output: CSV per keyword → digabung → deduplikasi by `id_str`
   - Upload sebagai Parquet ke MinIO: `twitter-raw/tweets/YYYY/MM/DD/HH_MM.parquet`
   - Log ke tabel `pipeline_lineage` (run_id, rows_processed, params JSONB)

2. **`score_and_store()`** — baca Parquet dari MinIO
   - Scoring FinVADER per baris teks tweet → kolom `compound` (-1 sampai +1)
   - Hitung agregasi per window 30 menit:
     - `compound_score` = rata-rata compound
     - `positive_ratio`, `negative_ratio`, `neutral_ratio`
     - `weighted_compound` = compound dikali jumlah like, dinormalisasi
     - `data_quality`: `ok` (≥5 tweet), `low_sample` (<5 tweet)
   - UPSERT ke `sentiment_30m` via PgBouncer (`ON CONFLICT DO UPDATE`)

**Jadwal:** setiap 30 menit — `IntervalSchedule(interval=1800)` di `deploy_all.py`

**Kenapa ini batch (bukan stream):**
- Data dikumpulkan dulu seluruhnya ke MinIO, baru diproses sekaligus
- Idempotent: bisa diulang tanpa duplikasi (ON CONFLICT DO UPDATE)
- Lake-first pattern: raw Parquet di MinIO bisa di-query ulang via Trino

**Dependencies:** `prefect/requirements.txt` — `finvader`, `pandas`, `pyarrow`, `s3fs`, `minio`, `pybreaker`

---

## 3. Stream Processing (15)

**Implementasi:** `processing/stream_processor.py` — Spark Structured Streaming

**Alur stream:**

1. Subscribe Kafka topic `btc_ticker_raw` (3 partisi)
2. Parse JSON message: `{event_time, symbol, trade_id, price, quantity, is_buyer_mm}`
3. Cast price/quantity ke DOUBLE, filter baris null
4. **Tumbling window 1 menit** — agregasi OHLC:
   - `open` = harga pertama dalam window
   - `high` = harga maksimum
   - `low` = harga minimum
   - `close` = harga terakhir
   - `volume` = total quantity
   - `trade_count` = jumlah trade
   - `volatility` = std log-return (custom UDF)
5. UPSERT ke PostgreSQL `btc_ohlc_1m` via JDBC → PgBouncer (port 6432)
6. Log lineage ke `pipeline_lineage` setiap micro-batch selesai

**Fault tolerance:**
- **Circuit breaker** (pybreaker): 5 kali gagal berturut → open 60 detik → kirim Telegram alert
- **Checkpoint** di MinIO `s3a://checkpoints/spark-streaming/` → recovery otomatis jika restart
- **DLQ** (Dead Letter Queue): pesan malformed → Kafka topic `btc_ticker_dlq`
- **Binance producer**: exponential backoff 5x retry, DNS-over-HTTPS bypass untuk ISP hijacking

**Kenapa ini stream (bukan batch):**
- Latensi sub-second dari trade Binance sampai baris di PostgreSQL
- State window dijaga Spark Structured Streaming (watermark, offset tracking)
- Tidak perlu simpan raw data — langsung agregasi dan discard

---

## 4. Integrasi dengan Machine Learning (15)

**Implementasi:** `prefect/flows/model_training.py`

**Alur integrasi:**

1. **`check_data_availability()`** — cek minimum data:
   - `btc_ohlc_1m` ≥ 1000 baris
   - `sentiment_30m` ≥ 48 baris (setara 24 jam data 30-menitan)

2. **`prepare_features()`** — feature engineering via SQL JOIN:
   ```sql
   -- Join OHLC (1 menit) dengan sentimen (30 menit)
   -- OHLC masuk ke window sentimen yang mencakupnya
   LEFT JOIN sentiment_30m s
     ON o.window_start >= s.window_start
    AND o.window_start <  s.window_start + INTERVAL '30 minutes'
   ```
   Fitur OHLC: `rolling_vol_5m`, `price_range_ratio`, `vol_ratio`
   Fitur sentimen: `compound_score` (avg_sentiment), `tweet_count` (mention_count), `positive_ratio`
   Target: `target_vol_5m` (5-menit forward volatility)
   Output: `/tmp/btc_training_features.parquet`

3. **`train_model()`** — XGBoost Regressor:
   - Params: 200 estimators, max_depth=6, learning_rate=0.05, subsample=0.8
   - Split 80/20 tanpa shuffle (time series, urutan penting)
   - MLflow tracking: params, RMSE, MAE, feature importances per fitur
   - Daftar model ke MLflow Registry sebagai `btc-volatility-xgboost`
   - Promote versi terbaru ke stage **Production** (arsip versi lama)

4. **`log_audit()`** — INSERT ke `pipeline_lineage`:
   - source: `btc_ohlc_1m + sentiment_30m`
   - params JSONB: `{rmse, mlflow_run_id}`

**Integrasi ke stream processor:**
- `stream_processor.py` load model Production dari MLflow saat startup
- Fallback ke cache lokal `/tmp/xgb_model_cache/model.pkl` jika MLflow tidak tersedia
- Model siap dipakai untuk inference volatility real-time

**Jadwal retraining:** setiap Senin jam 02:00 UTC — `CronSchedule(cron="0 2 * * 1")`

---

## 5. Visualisasi & Dashboard (10)

**Grafana** — `monitoring/grafana/dashboards/`
- `pipeline-monitoring.json`: row count `btc_ohlc_1m` dan `sentiment_30m`, execution time pipeline, status quality
- `node-metrics.json`: resource utilization (CPU, memory node)
- Datasource: PostgreSQL (`btcdb`) di-provision otomatis dari `monitoring/grafana/provisioning/`
- Alert rules: dapat dikonfigurasi langsung di Grafana UI, notifikasi ke Telegram

**Streamlit** — `dashboard/`
- Query via Trino sebagai unified query engine (PostgreSQL + MinIO)
- Rencana tampilan: candlestick OHLC, sentiment time series, correlation sentiment-volatility, model prediction

**Trino sebagai query engine (mandatory sesuai spesifikasi dosen):**
- Catalog `postgresql` → direct query ke `btc_ohlc_1m`, `sentiment_30m`, `pipeline_lineage`
- Catalog `hive` → query Parquet raw tweets di MinIO via Hive Metastore
- Memungkinkan satu query SQL JOIN antara data PostgreSQL dan file di MinIO

---

## 6. Monitoring & Logging (10)

**Data lineage** — tabel `pipeline_lineage` (PostgreSQL):

| Kolom | Isi |
|---|---|
| `run_id` | UUID unik per run |
| `pipeline_name` | `sentiment_pipeline` / `model_training` / `btc-stream-processor` |
| `source` | Asal data (keyword Twitter / Kafka topic / tabel JOIN) |
| `target_table` | Tabel tujuan write |
| `rows_processed` | Jumlah baris yang diproses |
| `rows_rejected` | Jumlah baris ditolak (default 0) |
| `quality_status` | `ok` / `failed` / `stale` |
| `started_at` / `finished_at` | Timestamp durasi run |
| `params` | JSONB — metadata fleksibel (path file, keywords, RMSE, dll) |

Diisi oleh: `sentiment_flow.py` (task `harvest_tweets`), `stream_processor.py` (tiap micro-batch), `model_training.py` (task `log_audit`)

**Audit trail otomatis** — tabel `audit_log` (PostgreSQL):
- PostgreSQL trigger `fn_audit_trigger()` aktif di: `btc_ohlc_1m`, `sentiment_30m`, `pipeline_lineage`
- Setiap INSERT/UPDATE/DELETE tercatat: `old_data` (JSONB), `new_data` (JSONB), `changed_by` (db user), `changed_at`

**Application logging:**
- `binance_producer.py`: Python `logging` level INFO/WARNING/ERROR → stdout → `docker logs binance-producer`
- `stream_processor.py`: Spark logging + custom logger `stream_processor` → `docker logs spark-streaming-job`
- `sentiment_flow.py`: Prefect structured logging per task → Prefect UI (`http://localhost:4200`)

---

## 7. Alerting System (10)

**Mekanisme:** Telegram Bot — env `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID`

**Titik alert A — Binance Producer** (`ingestion/binance_producer.py`):
- WebSocket disconnect + semua retry habis (5x exponential backoff) → alert "koneksi Binance gagal"
- Pesan dikirim ke DLQ (`btc_ticker_dlq`) → alert "data malformed terdeteksi"

**Titik alert B — Sentiment Flow** (`prefect/flows/sentiment_flow.py`):
- Circuit breaker ke PostgreSQL OPEN (5 kali gagal berturut-turut, reset 60 detik):
  ```
  [sentiment_flow] Circuit breaker OPEN: PostgreSQL gagal 5x
  ```
- Flow `sentiment_pipeline` gagal setelah 3x retry (delay 5 menit per retry):
  ```
  [sentiment_pipeline] FAILED untuk window 2026-06-13 01:30:00: <error detail>
  ```
- Setelah alert: forward-fill otomatis — baris window terakhir yang sukses di-copy ke window gagal dengan `data_quality='stale'` agar pipeline downstream tidak crash

**Konfigurasi:** isi `TELEGRAM_BOT_TOKEN` dan `TELEGRAM_CHAT_ID` di `.env`

---

## 8. Keamanan Data (10)

**Authentication & Authorization:**
- PostgreSQL: dua role terpisah
  - `kelompok4_ipbd` — full write access (dipakai pipeline)
  - `dashboard_reader` — SELECT only pada `btc_ohlc_1m`, `sentiment_30m`, `pipeline_lineage`, `audit_log` (dipakai Grafana/Streamlit)
- PgBouncer: autentikasi via `pgbouncer/userlist.txt` (password md5), `POOL_MODE=transaction` — aplikasi tidak pegang koneksi PostgreSQL langsung
- MinIO: akses hanya dengan access key + secret key (tidak ada anonymous access)
- Grafana: login admin via env `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD`
- Prefect: API hanya accessible dalam Docker network `bigdata_net`

**Network isolation:**
- Semua service dalam Docker bridge network `bigdata_net` — tidak bisa diakses dari luar tanpa port mapping eksplisit
- Port yang di-expose ke host hanya untuk kebutuhan observasi (5434, 9001, 4200, 3000, dll)
- Service internal (PgBouncer, Hive Metastore, Zookeeper) tidak expose ke host

**Nginx reverse proxy** (`security/nginx/`):
- Self-signed SSL cert (`nginx.crt`, `nginx.key`) untuk HTTPS
- Basic auth via `.htpasswd` untuk akses unified ke semua dashboard
- Siap diaktifkan — tinggal uncomment service di `docker-compose.yml`

**Data protection:**
- Semua kredensial via environment variable dari `.env` — tidak ada hardcode di kode
- `.env` tidak di-commit ke repository (template: `.env.example`)
- PgBouncer `POOL_MODE=transaction` mencegah connection leakage antar request

---

## 9. Governance Big Data (5)

**Data Quality — validasi inline di `sentiment_flow.py`:**
```python
# Sebelum UPSERT ke PostgreSQL, wajib lolos:
- Tidak ada null di kolom teks dan compound score
- compound_score harus dalam range [-1, 1]
- Jika gagal validasi → raise ValueError → data tidak masuk DB
```

**Data Quality flag — kolom `data_quality` di `sentiment_30m`:**
| Nilai | Kondisi |
|---|---|
| `ok` | Normal, ≥5 tweet dalam window |
| `low_sample` | Kurang dari 5 tweet, score kurang representatif |
| `stale` | Window gagal diproses, diisi forward-fill dari window terakhir |

**Schema constraints di `security/postgres/init.sql`:**
- `btc_ohlc_1m`: CHECK `open > 0`, `high >= low`, `high >= open`, `volume >= 0`, `UNIQUE(window_start)`
- `sentiment_30m`: CHECK `compound_score BETWEEN -1 AND 1`, `positive/negative/neutral_ratio BETWEEN 0 AND 1`, `UNIQUE(window_start)`
- `audit_log`: CHECK `operation IN ('INSERT','UPDATE','DELETE')`

**Metadata & Lineage:**
- `pipeline_lineage`: setiap pipeline run tercatat dengan source, target, jumlah rows, params JSONB, dan status quality
- `audit_log`: setiap perubahan data di tabel utama tercatat otomatis via PostgreSQL trigger (tidak bisa di-bypass)
- MinIO path `tweets/YYYY/MM/DD/HH_MM.parquet` — terstruktur waktu, queryable via Trino catalog hive

---

## 10. Dokumentasi & Reproducibility (5)

**Cara menjalankan ulang dari nol:**

```bash
# 1. Setup environment
cp .env.example .env
# Edit .env — wajib diisi:
#   TWITTER_AUTH_TOKEN  → auth token dari browser Twitter/X
#   TELEGRAM_BOT_TOKEN  → dari @BotFather di Telegram
#   TELEGRAM_CHAT_ID    → ID chat tujuan alert

# 2. Build image dan jalankan semua service
docker compose up -d --build

# 3. Cek semua service healthy (~3-5 menit)
docker compose ps
# Semua harus: healthy atau running
# kafka-init, minio-init, prefect-init boleh: exited (0)

# 4. Verifikasi Binance → PostgreSQL (stream berjalan otomatis)
docker exec -it postgres psql -U kelompok4_ipbd -d btcdb \
  -c "SELECT COUNT(*), MAX(window_start) FROM btc_ohlc_1m;"
# Tunggu 1-2 menit, harus ada rows

# 5. Trigger sentiment pipeline (atau tunggu 30 menit otomatis)
# Buka http://localhost:4200 → Deployments → sentiment-pipeline → Quick Run

# 6. Verifikasi sentimen → PostgreSQL
docker exec -it postgres psql -U kelompok4_ipbd -d btcdb \
  -c "SELECT window_start, compound_score, tweet_count, data_quality FROM sentiment_30m ORDER BY window_start DESC LIMIT 5;"

# 7. Akses semua dashboard
# Prefect UI   : http://localhost:4200
# Grafana      : http://localhost:3000   (kelompok4_ipbd / k4ipbd_grafana_2026)
# MLflow       : http://localhost:5000
# MinIO Console: http://localhost:9001   (minioadmin / k4ipbd_minio_2026)
# Trino UI     : http://localhost:8082
# Spark UI     : http://localhost:8081
```

**Struktur direktori:**

```
bitcoin-volatility-ml/
├── ingestion/
│   ├── binance_producer.py     # WebSocket Binance → Kafka
│   └── Dockerfile.producer
├── processing/
│   ├── stream_processor.py     # Spark Streaming: Kafka → OHLC → PostgreSQL
│   └── entrypoint.sh           # spark-submit wrapper
├── prefect/
│   ├── flows/
│   │   ├── sentiment_flow.py   # tweet-harvest → FinVADER → sentiment_30m
│   │   ├── model_training.py   # XGBoost training → MLflow
│   │   └── deploy_all.py       # register deployments ke Prefect
│   ├── Dockerfile
│   └── requirements.txt
├── security/
│   ├── postgres/
│   │   ├── init.sql            # schema lengkap + audit trigger
│   │   └── create_multiple_db.sh
│   └── nginx/                  # SSL cert, basic auth
├── monitoring/grafana/
│   ├── dashboards/             # pipeline-monitoring.json, node-metrics.json
│   └── provisioning/           # datasource auto-provision
├── trino/catalog/
│   ├── postgresql.properties   # catalog PostgreSQL
│   └── hive.properties         # catalog MinIO via Hive Metastore
├── pgbouncer/
│   ├── pgbouncer.ini
│   └── userlist.txt
├── governance/
│   └── great_expectations/     # konfigurasi GE
├── dashboard/                  # Streamlit app
├── docker-compose.yml
├── .env.example
└── SKILLS.md
```

**Port semua service:**

| Service | Port Host | Fungsi |
|---|---|---|
| PostgreSQL | 5434 | Database utama (btcdb, mlflowdb, prefectdb, dll) |
| PgBouncer | 6432 | Connection pooler PostgreSQL |
| MinIO API | 9000 | S3-compatible object storage (akses programatik) |
| MinIO Console | 9001 | Web UI browser object storage |
| Kafka | 9092 | Message broker (akses dari host) |
| Spark Master UI | 8081 | Monitoring job Spark |
| MLflow | 5000 | ML experiment tracking + model registry |
| Prefect | 4200 | Workflow orchestration + scheduling UI |
| Trino | 8082 | Query engine (PostgreSQL + MinIO) |
| Grafana | 3000 | Dashboard monitoring pipeline |
| Hive Metastore | 9083 | Schema registry untuk Parquet di MinIO |
