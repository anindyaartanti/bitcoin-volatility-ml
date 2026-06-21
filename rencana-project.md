# Prediksi Volatilitas Bitcoin Menggunakan Analisis Sentimen Media Sosial secara Real-Time

Proyek ini membangun pipeline Big Data **end‑to‑end** untuk memprediksi volatilitas Bitcoin dengan menggabungkan data harga real‑time dari Binance (stream) dan analisis sentimen dari Twitter/X (batch). Pipeline berjalan sepenuhnya di lingkungan lokal tanpa ketergantungan cloud provider, menggunakan container Docker dan orkestrasi Apache Airflow. Proyek ini dikembangkan untuk memenuhi **Tugas Besar Praktikum Big Data** yang mencakup seluruh siklus hidup data: ingestion, batch/stream processing, machine learning, visualisasi, monitoring, alerting, keamanan, dan tata kelola.

---

## 1. Deskripsi Proyek

Bitcoin dikenal dengan volatilitas harganya yang tinggi. Memprediksi fluktuasi ini dapat membantu investor dalam pengambilan keputusan. Proyek ini memanfaatkan dua sumber data:

- **Stream (real‑time)**: data harga dan volume Bitcoin dari Binance WebSocket.
- **Batch (periodik)**: data teks dari Twitter/X menggunakan tweet-harvest untuk analisis sentimen.

Kedua aliran data diproses secara paralel, kemudian digabungkan untuk menghasilkan fitur yang menjadi input model machine learning (XGBoost Regressor). Model memprediksi volatilitas (standar deviasi return) dalam jangka pendek (5 menit ke depan). Hasil prediksi ditampilkan di dashboard, sementara seluruh sistem dipantau secara real‑time dengan alerting ke Telegram.

---

## 2. Arsitektur Pipeline

### 2.1 Diagram Arsitektur

```
+---------------------------------------------------------------------------------------------------------------------+
|                                                  DATA SOURCES                                                       |
|                                                                                                                     |
|   +-----------------------------+                          +------------------------------+                         |
|   | Binance WebSocket (Stream)  |                          |   Twitter/X (Batch)          |                         |
|   |                             |                          |   via tweet-harvest          |                         |
|   +--------------+--------------+                          +---------------+--------------+                         |
|                  |                                                         |                                        |
+---------------------------------------------------------------------------------------------------------------------+
                   |                                                         |
                   v                                                         v
+------------------------------------+                     +----------------------------------------+
|            KAFKA BROKER            |                     |          AIRFLOW SCHEDULER             |
|  Topic: btc_ticker_raw             |                     |  DAG: twitter_ingestion                |
|  Topic: volatility_pred            |                     |  DAG: sentiment_processing             |
+------------------+-----------------+                     |  DAG: model_training                   |
                   |                                       +-----------------+----------------------+
                   |                                                         |
                   v                                                         v
+-------------------------------------+                  +----------------------------------------+
|      SPARK STRUCTURED STREAMING     |                  |          MINIO DATA LAKE               |
|  - Windowed aggregation (1-min OHLC)|                  |  Raw Twitter posts (Parquet)           |
|  - Rolling volatility (5-min)       |                  |  spark-checkpoints                     |
|  - Feature engineering              |                  |  mlflow-artifacts                      |
|  - Model inference (XGBoost UDF)    |                  +--------------------+-------------------+
+------------------+------------------+                                       |
                   |                                                          v
                   |                                       +----------------------------------------+
                   |                                       |             SPARK BATCH                |
                   |                                       |  - Text preprocessing                  |
                   |                                       |  - VADER sentiment scoring             |
                   |                                       |  - Hourly aggregation                  |
                   |                                       +--------------------+-------------------+
                   |                                                            |
                   +----------------------+   +---------------------------------+
                                          |   |
                                          v   v
+----------------------------------------------------------------------------------------+
|                                   POSTGRESQL DATABASE                                  |
|  - btc_ohlc_1m        (dari Spark Streaming)                                          |
|  - sentiment_hourly   (dari Spark Batch)                                              |
|  - predictions        (dari Model inference)                                          |
|  - audit_log                                                                          |
|  - metadata_table                                                                     |
+-----+-------------------------------------------+-------------------------------+----+
      |                                           |                               |
      |                              +------------+------------+                  |
      |                              |   HIVE METASTORE        |                  |
      |                              |   (skema Parquet        |                  |
      |                              |    di MinIO)            |                  |
      |                              +------------+------------+                  |
      |                                           |                               |
      |                              +------------+------------+                  |
      |      PostgreSQL connector ───+          TRINO          |                  |
      +──────────────────────────────+  Hive connector (MinIO) |                  |
                                     +------------+------------+                  |
                                                  |                               |
                   +--------------------------+   |                               |
                   |                          |   |                               |
                   v                          v   v                               v
      +------------+----------+   +----------+---+-------+           +-----------+---------+
      |  GRAFANA DASHBOARD    |   |  STREAMLIT DASHBOARD  |           |   AIRFLOW WEB UI    |
      |  - Harga & volatilitas|   |  - Harga & volatilitas|           |   (status DAG)      |
      |  - Gauge sentimen     |   |  - Gauge sentimen     |           +---------------------+
      |  - Riwayat prediksi   |   |  - Riwayat prediksi  |
      |  datasource:          |   |  datasource: Trino    |
      |  PostgreSQL langsung  |   +----------------------+
      +----------+------------+
                 |
                 v
         +-------+--------+
         |    TELEGRAM     |
         |  (Grafana Alert)|
         +----------------+
```

**Komponen Pendukung:**

```
+--------------------------------------------------------------------------------------------------+
|                         SUPPORTING SERVICES & SECURITY                                           |
|                                                                                                  |
|  +---------------------+  +------------------------+  +-----------------------------+            |
|  | Container Orchest.  |  | Governance & Quality   |  | Security                    |            |
|  | Docker Compose      |  | - Great Expectations   |  | - Nginx reverse proxy       |            |
|  | (semua service)     |  | - Metadata table (PG)  |  |   (Basic Auth + TLS)        |            |
|  +---------------------+  | - Audit logging (PG)   |  | - PostgreSQL RBAC           |            |
|                           +------------------------+  | - Kafka ACL                 |            |
|                                                       +-----------------------------+            |
|  +---------------------+  +------------------------+                                             |
|  | MLflow Tracking &   |  | Hive Metastore         |                                             |
|  | Model Registry      |  | (PostgreSQL backend)   |                                             |
|  | (XGBoost artifacts) |  |                        |                                             |
|  +---------------------+  +------------------------+                                             |
+--------------------------------------------------------------------------------------------------+
```

### 2.2 Penjelasan Layer

1. **Data Source**:
   - **Binance WebSocket** mengirimkan harga dan volume real‑time untuk pasangan BTC/USDT.
   - **Twitter/X** diakses secara periodik (setiap 6 jam) menggunakan **tweet-harvest**, yaitu library Node.js berbasis Playwright yang melakukan scraping tanpa API key resmi, cukup dengan `auth_token` cookie dari browser.

2. **Ingestion**:
   - Data stream dikirim ke **Apache Kafka** (topik `btc_ticker_raw`) melalui producer Python.
   - Data batch diambil oleh Airflow DAG `twitter_ingestion` menggunakan tweet-harvest, disimpan sebagai file Parquet di **MinIO** bucket `twitter-raw`.

3. **Processing**:
   - **Stream Processing** (Spark Structured Streaming) membaca dari Kafka, melakukan windowing 1 menit untuk menghitung OHLC, rolling volatility, dan menerapkan model ML untuk prediksi langsung.
   - **Batch Processing** (Spark Batch) membaca Parquet dari MinIO, menjalankan text preprocessing, sentimen analisis VADER, agregasi per jam, dan menyimpan skor sentimen ke PostgreSQL.

4. **Storage**:
   - **MinIO** sebagai data lake (tweet mentah, artefak model, Spark checkpoints).
   - **PostgreSQL** sebagai database utama untuk data terstruktur (OHLC, sentimen, prediksi, audit, metadata). Menjalankan tiga database sekaligus: aplikasi, Airflow metadata, dan MLflow backend.

5. **Query Layer**:
   - **Trino** duduk di atas PostgreSQL dan MinIO sebagai query engine terpadu.
   - Menggunakan **PostgreSQL connector** untuk query tabel operasional dan **Hive connector** (dengan Hive Metastore) untuk query file Parquet di MinIO.
   - **Hive Metastore** menyimpan definisi skema tabel eksternal yang merujuk ke file Parquet di MinIO, menggunakan PostgreSQL sebagai backend-nya.

6. **Machine Learning**:
   - Model XGBoost dilatih secara batch dengan data dari PostgreSQL.
   - Training di-tracking dengan **MLflow**, model terbaik disimpan di Model Registry dan di-load oleh Spark untuk inference stream.

7. **Dashboard**:
   - **Streamlit** menampilkan dashboard bisnis, query data melalui **Trino** (federated: PostgreSQL + MinIO).
   - **Grafana** digunakan untuk dashboard monitoring dan alert, query langsung ke **PostgreSQL**.

8. **Alerting**:
   - **Grafana Alerting** (built-in) mengirim notifikasi ke **Telegram Bot** saat terjadi anomali.

9. **Security**:
   - **Nginx** sebagai reverse proxy dengan basic authentication dan TLS.
   - PostgreSQL RBAC dan Kafka ACL.

10. **Governance**:
    - **Great Expectations** memvalidasi kualitas data.
    - Metadata, lineage, dan audit trail dicatat di PostgreSQL.

11. **Orchestration**:
    - **Docker Compose** untuk semua service.
    - **Apache Airflow** untuk penjadwalan pipeline batch.

---

## 3. Teknologi yang Digunakan (Tech Stack)

| Kategori               | Tools & Teknologi                                                          |
|------------------------|----------------------------------------------------------------------------|
| **Infrastruktur**      | Docker, Docker Compose                                                     |
| **Stream Ingestion**   | Binance WebSocket, Apache Kafka                                            |
| **Batch Ingestion**    | tweet-harvest (Node.js + Playwright), Apache Airflow                       |
| **Stream Processing**  | Apache Spark Structured Streaming                                          |
| **Batch Processing**   | Apache Spark Batch                                                         |
| **Database**           | PostgreSQL (app, airflow, mlflow, metastore)                               |
| **Data Lake**          | MinIO (S3‑compatible object storage)                                       |
| **Query Engine**       | Trino, Hive Metastore                                                      |
| **Machine Learning**   | XGBoost, MLflow (tracking & registry)                                      |
| **Bahasa Pemrograman** | Python 3.11+ (pyspark, kafka-python, vaderSentiment, dll.), Node.js        |
| **Visualisasi**        | Streamlit (via Trino), Grafana (via PostgreSQL)                            |
| **Alerting**           | Grafana Alerting built-in → Telegram Bot                                   |
| **Keamanan**           | Nginx (basic auth + TLS), PostgreSQL RBAC, Kafka ACL                       |
| **Data Governance**    | Great Expectations, metadata & audit tables (PostgreSQL)                   |
| **Version Control**    | Git, GitHub                                                                |

---

## 4. Struktur Folder Proyek

```
bitcoin-volatility-ml/
│
├── docker-compose.yml                # Orkestrasi semua container
├── .env                              # Environment variables (kredensial, port, path)
├── README.md                         # Dokumentasi proyek
│
├── airflow/                          # Apache Airflow
│   ├── dags/
│   │   ├── twitter_ingestion.py      # DAG: scrape Twitter → MinIO (Parquet)
│   │   ├── sentiment_processing.py   # DAG: Spark batch sentiment scoring
│   │   └── model_training.py         # DAG: training XGBoost & register MLflow
│   └── plugins/                      # Custom Airflow plugins
│
├── ingestion/                        # Script ingestion
│   ├── Dockerfile.producer           # Image untuk binance-producer container
│   ├── binance_producer.py           # Stream: Binance WebSocket → Kafka
│   ├── csv_to_minio.py               # Helper: CSV tweet-harvest → Parquet → MinIO
│   └── requirements.txt              # Deps ingestion (kafka-python, websocket, dll.)
│
├── processing/                       # Spark jobs
│   ├── stream_processor.py           # Spark Streaming: Kafka → OHLC → inference
│   ├── batch_sentiment.py            # Spark Batch: MinIO Parquet → VADER → PostgreSQL
│   └── config.py                     # Konfigurasi Spark (MinIO endpoint, JDBC)
│
├── ml/                               # Machine Learning
│   ├── train_model.py                # Training XGBoost, tracking MLflow
│   ├── prepare_data.py               # Query fitur dari PostgreSQL via Trino
│   └── model_loader.py               # Load model dari MLflow Registry
│
├── dashboard/                        # Streamlit
│   ├── app.py                        # Aplikasi Streamlit (query via Trino)
│   ├── config.py                     # Koneksi Trino
│   └── assets/                       # CSS / gambar statis
│
├── trino/                            # Konfigurasi Trino
│   ├── catalog/
│   │   ├── postgresql.properties     # Connector: Trino → PostgreSQL
│   │   └── hive.properties           # Connector: Trino → MinIO (via Hive Metastore)
│   └── config.properties             # Konfigurasi Trino server
│
├── monitoring/                       # Konfigurasi monitoring
│   └── grafana/
│       ├── dashboards/
│       │   └── pipeline-monitoring.json   # Dashboard JSON siap import
│       └── provisioning/
│           └── datasources/
│               └── datasources.yaml  # Datasource: PostgreSQL
│
├── security/                         # Keamanan
│   ├── nginx/
│   │   ├── nginx.conf                # Reverse proxy & basic auth
│   │   ├── .htpasswd                 # File password basic auth (jangan di-commit)
│   │   ├── nginx.crt                 # Self-signed TLS certificate
│   │   └── nginx.key
│   └── postgres/
│       ├── init.sql                  # Inisialisasi user, role, tabel
│       └── create_multiple_db.sh     # Script buat multi-database
│
├── docker/                           # Custom Dockerfiles
│   └── tweet-harvest/
│       ├── Dockerfile                # Node.js + Playwright + tweet-harvest
│       └── entrypoint.sh             # Runner: harvest per keyword → CSV
│
└── governance/                       # Data quality & audit
    └── great_expectations/
        ├── expectations/             # Suite expectations JSON
        └── great_expectations.yml
```

---

## 5. Detail Komponen

### 5.1 Data Source & Ingestion

**Stream (Binance WebSocket → Kafka)**
- **Script**: `ingestion/binance_producer.py`
- Menghubungkan ke `wss://stream.binance.com:9443/ws/btcusdt@trade`.
- Setiap tick diformat ke JSON dan dikirim ke Kafka topic `btc_ticker_raw` (internal: `kafka:29092`).
- Producer menggunakan mekanisme async dengan graceful shutdown (SIGTERM/SIGINT).

**Batch (Twitter/X → MinIO via tweet-harvest + Airflow)**
- **tweet-harvest** adalah CLI Node.js berbasis Playwright yang scrape Twitter tanpa API key resmi.
- Cara dapat `auth_token`: Login ke x.com → DevTools → Application → Cookies → salin nilai `auth_token`.
- Airflow DAG `twitter_ingestion` menjalankan container tweet-harvest (via DockerOperator), menghasilkan CSV per keyword.
- Helper `ingestion/csv_to_minio.py` mengkonversi CSV → Parquet dan upload ke MinIO bucket `twitter-raw`.
- Metadata (waktu, jumlah tweet) ditulis ke tabel `metadata_table` di PostgreSQL.

### 5.2 Data Processing

**Stream Processing (Spark Structured Streaming)**
- File: `processing/stream_processor.py`
- Membaca dari Kafka topic `btc_ticker_raw`.
- Windowing tumbling 1 menit → OHLC, rolling volatility 5 menit, feature engineering.
- Join dengan sentimen terbaru dari PostgreSQL (`sentiment_hourly`) via JDBC.
- Inference XGBoost via MLflow UDF → hasil ke PostgreSQL (`predictions`) dan Kafka topic `volatility_pred`.
- Checkpoint disimpan di MinIO bucket `spark-checkpoints`.

**Batch Processing (Spark Batch)**
- File: `processing/batch_sentiment.py`
- Dipanggil Airflow DAG `sentiment_processing` setiap 6 jam setelah `twitter_ingestion` selesai.
- Membaca Parquet dari MinIO (`s3a://twitter-raw/`).
- Preprocessing teks → VADER sentiment scoring → agregasi per jam.
- Hasil ke PostgreSQL tabel `sentiment_hourly`.
- Validasi data dengan Great Expectations sebelum penulisan.

### 5.3 Storage

**MinIO (Data Lake)**
- API: `http://minio:9000`, Console: `http://localhost:9003`.
- Bucket:
  - `twitter-raw` — Parquet mentah hasil tweet-harvest.
  - `mlflow-artifacts` — artefak model dari MLflow.
  - `spark-checkpoints` — checkpoint Spark Streaming.

**PostgreSQL**
- Internal port: 5432, external: 5434.
- Database:
  - `btcdb` — data aplikasi (OHLC, sentimen, prediksi, audit, metadata).
  - `airflow` — metadata Airflow.
  - `mlflow` — backend MLflow.
  - `metastore` — backend Hive Metastore (untuk Trino).
- Role: `dashboard_reader` (SELECT only), `ml_writer` (INSERT/UPDATE).

### 5.4 Query Layer (Trino + Hive Metastore)

**Trino**
- Query engine terpadu yang bisa query PostgreSQL dan MinIO dalam satu SQL.
- Connector `postgresql`: akses ke semua tabel di database `btcdb`.
- Connector `hive`: akses ke file Parquet di MinIO melalui Hive Metastore.
- Digunakan oleh Streamlit sebagai satu-satunya datasource.

**Hive Metastore**
- Menyimpan definisi skema tabel eksternal (nama kolom, tipe data, lokasi di MinIO).
- Backend: PostgreSQL database `metastore`.
- Diperlukan agar Trino tahu struktur file Parquet di MinIO.

### 5.5 Machine Learning Pipeline

**Alur Training**
1. `ml/prepare_data.py`: Query fitur dari PostgreSQL via Trino — join `btc_ohlc_1m` dan `sentiment_hourly`.
2. `ml/train_model.py`: Training XGBRegressor, tracking MLflow (parameter, RMSE, MAE, model).
3. Model terbaik diregistrasi ke MLflow Model Registry (stage "Production").
4. Inference: model di-load via `mlflow.pyfunc` dan digunakan sebagai Spark UDF di stream processor.

**MLflow**
- Metadata di PostgreSQL database `mlflow`, artefak di MinIO bucket `mlflow-artifacts`.
- UI di port 5000.

### 5.6 Dashboard & Visualisasi

**Streamlit (Dashboard Bisnis)**
- `dashboard/app.py` query data via **Trino** (bukan langsung ke PostgreSQL).
- Menampilkan: grafik harga BTC, prediksi volatilitas, gauge sentimen, tabel tweet terkini.
- Auto-refresh setiap 10 detik.
- Akses via Nginx (HTTPS + basic auth).

**Grafana (Dashboard Monitoring)**
- Datasource: **PostgreSQL langsung** (bukan Trino).
- Panel: harga BTC real-time, lag sentimen, status prediksi, jumlah data masuk.
- Alert: kondisi anomali → notifikasi **Telegram** via Grafana built-in alert contact point.
- Tidak menggunakan Prometheus, Loki, atau Promtail.

### 5.7 Alerting

- Grafana Alerting (built-in) terhubung ke **Telegram Bot**.
- Rule contoh:
  - `predicted_volatility > 5%` → notifikasi anomali volatilitas.
  - Tidak ada data baru di `btc_ohlc_1m` selama 5 menit → pipeline stream bermasalah.
  - Tidak ada data baru di `sentiment_hourly` setelah jadwal batch → pipeline batch bermasalah.
- Token bot dan chat ID disimpan di `.env`.

### 5.8 Keamanan

1. **Autentikasi**:
   - Dashboard (Streamlit + Grafana + Airflow): dilindungi basic auth via Nginx.
   - PostgreSQL: RBAC (`dashboard_reader`, `ml_writer`).
   - Kafka: ACL per topik.

2. **Enkripsi**:
   - TLS via Nginx (self-signed certificate untuk development).

3. **Kredensial**: disimpan di `.env`, tidak di-commit ke Git.

### 5.9 Governance Big Data

**Data Quality**
- Great Expectations memvalidasi data tweet sebelum diproses Spark.
- Checks: kolom teks tidak null, tidak duplikasi tweet ID, timestamp valid.

**Metadata & Lineage**
- `metadata_table`: pipeline name, run ID, source, destination, row count, quality passed.
- Lineage: `Twitter/X → tweet-harvest → MinIO → Spark Batch → sentiment_hourly → predictions`.

**Audit Trail**
- `audit_log`: timestamp, username, action, table_name, record_id, details.
- Implementasi via trigger PostgreSQL atau insert manual dari aplikasi.

**Retensi Data**
- Tweet mentah di MinIO dihapus otomatis setelah 90 hari via MinIO Lifecycle Policy.

### 5.10 Orchestration

**Docker Compose** — satu file mendefinisikan semua service dalam network `bigdata_net`.

**Airflow DAGs**:
- `twitter_ingestion`: setiap 6 jam → jalankan tweet-harvest → CSV ke MinIO.
- `sentiment_processing`: triggered setelah `twitter_ingestion` sukses → Spark batch.
- `model_training`: mingguan (atau manual) → training XGBoost → register MLflow.

---

## 6. Panduan Instalasi dan Menjalankan

### 6.1 Prasyarat

- Docker Engine ≥ 20.10
- Docker Compose v2
- RAM minimal 8 GB (16 GB direkomendasikan)
- Disk minimal 40 GB

### 6.2 Menjalankan Stack

```bash
# 1. Clone repository
git clone https://github.com/username/bitcoin-volatility-ml.git
cd bitcoin-volatility-ml

# 2. Konfigurasi environment
cp .env.example .env
nano .env   # isi TWITTER_AUTH_TOKEN, semua password, TELEGRAM_BOT_TOKEN

# 3. Jalankan stack
docker compose up -d
```

**Akses UI:**

| Service         | URL                          | Login              |
|-----------------|------------------------------|--------------------|
| Airflow         | http://localhost:8080        | dari `.env`        |
| MinIO Console   | http://localhost:9003        | dari `.env`        |
| MLflow          | http://localhost:5001        | —                  |
| Spark Master    | http://localhost:8081        | —                  |
| Trino           | http://localhost:8082        | —                  |
| Grafana         | https://localhost/grafana    | dari `.env`        |
| Streamlit       | https://localhost/dashboard  | basic auth         |
