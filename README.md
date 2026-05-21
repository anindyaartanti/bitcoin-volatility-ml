# Bitcoin Volatility ML

Pipeline Big Data end-to-end untuk memprediksi volatilitas Bitcoin menggunakan harga real-time (Binance WebSocket) + analisis sentimen Twitter/X.

---

## Prasyarat

| Tool | Versi minimal | Cek |
|------|--------------|-----|
| Docker Desktop | 4.x | `docker --version` |
| Docker Compose | v2 | `docker compose version` |
| Python | 3.11+ | `python --version` |
| uv (package manager) | latest | `uv --version` |
| Node.js + npx | 18+ | `node --version` |
| Git | any | `git --version` |

Install `uv` jika belum ada:
```powershell
pip install uv
```

---

## Cara Menjalankan dari Awal

### 1. Clone Repository

```powershell
git clone https://github.com/USERNAME/bitcoin-volatility-ml.git
cd bitcoin-volatility-ml
```

### 2. Setup Environment

```powershell
make setup
```

Script ini akan:
- Menyalin `.env.example` → `.env`
- Men-generate `AIRFLOW_FERNET_KEY` dan `AIRFLOW_SECRET_KEY` otomatis

### 3. Install Dependensi Python

```powershell
uv pip install -r requirements.txt
uv pip install psycopg2-binary minio python-dotenv kafka-python
```

### 4. Jalankan Stack Docker

```powershell
make up
```

Tunggu semua container healthy (~2 menit). Cek status:

```powershell
make ps
```

Semua service yang harus `healthy`:

| Container | Status |
|-----------|--------|
| zookeeper | healthy |
| kafka | healthy |
| postgres | healthy |
| minio | healthy |
| airflow-webserver | healthy |
| airflow-scheduler | running |
| binance-producer | running |

### 5. Inisialisasi Airflow

Jalankan **sekali** setelah pertama kali `make up`:

```powershell
make airflow-init
```

Tunggu hingga muncul output `airflow already exist in the db` atau `Admin user created` (~3 menit).

Setelah selesai, jalankan ulang stack agar webserver ikut naik:

```powershell
make up
```

### 7. Verifikasi

```powershell
make verify
```

---

## URL Layanan

| Layanan | URL | Login |
|---------|-----|-------|
| Airflow | http://localhost:8080 | airflow / airflow |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |

```powershell
make open-all   # tampilkan semua URL sekaligus
```

---

## Perintah Make

```powershell
make setup              # Setup awal: buat .env + generate keys
make up                 # Jalankan semua service
make up-infra           # Jalankan hanya Kafka + PostgreSQL + MinIO
make down               # Hentikan semua
make ps                 # Status container
make logs               # Log semua (live)
make verify             # Verifikasi Fase 1 (10 pengecekan)
make airflow-init       # Init DB Airflow + buat user admin
make test-kafka         # Daftar topik Kafka
make test-kafka-consume # Baca 10 pesan dari btc_ticker_raw
make test-minio         # Daftar bucket MinIO
make test-postgres      # Daftar tabel PostgreSQL
make test-twitter       # Test scraping Twitter 5 tweet
make trigger-dag        # Trigger DAG twitter_ingestion manual
make open-all           # Tampilkan semua URL service
make clean              # Docker system prune
make help               # Tampilkan daftar perintah
```