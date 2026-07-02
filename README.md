# Bitcoin Volatility ML

End-to-end ML pipeline for real-time Bitcoin volatility prediction, combining Binance market data with Twitter sentiment analysis. Built with Kafka, Spark Structured Streaming, XGBoost, and a federated query layer via Trino.

---

## Architecture

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
  Prefect ─── Flow orchestration
  MLflow ──── Model registry
  Marquez ─── Data lineage
  Telegraf ── System metrics
```

## Tech Stack

| Category | Technology |
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
| **Metastore** | Hive Metastore (Parquet schema) |
| **Language** | Python 3.11, PySpark |
| **Container** | Docker, Docker Compose |

---

## Repository Structure

```
.
├── dashboard/                    # Streamlit dashboard (6 pages)
│   ├── app.py                    # Main app — Market Overview, Volatility,
│   │                             #   Sentiment, Cross-Source, Lineage, Operations
│   ├── config.py                 # DB & Trino connection config
│   ├── db.py                     # query_pg() & query_trino() helpers
│   ├── data_catalog.py           # Data Catalog tab (metadata, glossary)
│   ├── data_quality.py           # Data Quality tab (profiling stats)
│   ├── log_explorer.py           # Log Explorer tab (app_logs)
│   ├── ml_observability.py       # ML Observability tab (model perf)
│   ├── system_monitor.py         # System Monitor tab (containers, resources)
│   ├── Dockerfile
│   └── requirements.txt
│
├── ingestion/                    # Real-time data ingestion
│   ├── binance_producer.py       # Binance @trade → Kafka (w/ encryption)
│   ├── Dockerfile.producer
│   └── requirements.producer.txt
│
├── processing/                   # Spark Structured Streaming
│   ├── stream_processor.py       # Kafka → tumbling window 1m → OHLC + inference
│   ├── config.py                 # Spark & connection config
│   ├── Dockerfile                # Spark worker image
│   └── entrypoint.sh
│
├── prefect/                      # Workflow orchestration
│   └── flows/
│       ├── sentiment_flow.py     # Twitter scrape → FinVADER → PG (30min)
│       ├── training_flow.py      # Feature engineering → XGBoost → MLflow (daily)
│       ├── system_health_check.py # Container & host health (60s)
│       ├── data_quality_check.py # Great Expectations + profiling (hourly)
│       ├── model_performance_check.py # RMSE/MAE degradation check (hourly)
│       ├── log_ingester.py       # Log aggregation (5min)
│       └── deploy_all.py         # Register all deployments
│
├── ml/                           # ML training artifacts
│   ├── train_model.py            # Standalone training script
│   ├── model_loader.py           # Model cache loader (Spark-side)
│   ├── prepare_data.py           # Feature preparation
│   └── mlflow/
│       └── Dockerfile            # MLflow server image
│
├── governance/                   # Data governance
│   ├── audit_trail.sql           # (placeholder)
│   └── great_expectations/
│       ├── great_expectations.yml
│       └── expectations/         # GE suites for OHLC, sentiment, predictions
│
├── trino/                        # Trino (federated query engine)
│   ├── config.properties
│   ├── init-schema.sh
│   ├── catalog/
│   │   ├── postgresql.properties
│   │   └── hive.properties
│   └── ddl/
│       └── register_twitter_raw.sql
│
├── security/                     # Security & access control
│   ├── nginx/
│   │   ├── Dockerfile            # Reverse proxy (HTTPS + Basic Auth)
│   │   ├── nginx.conf
│   │   ├── entrypoint.sh
│   │   ├── .htpasswd             # (empty, gitignored)
│   │   ├── nginx.crt             # (gitignored)
│   │   └── nginx.key             # (gitignored)
│   └── postgres/
│       ├── init.sql              # Full schema, roles, grants, metadata seed
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
│       ├── telegraf.conf         # System metrics → PostgreSQL
│       └── scripts/
│           └── docker_status.sh
│
├── docker/                       # Docker support
│   └── hive/
│       ├── Dockerfile
│       └── entrypoint.sh
│
├── pgbouncer/                    # PostgreSQL connection pooler
│   ├── pgbouncer.ini
│   └── userlist.txt              # Contains MD5 password hash
│
├── scripts/                      # Utility scripts
│   ├── check_keys.py
│   ├── fetch_historical_btc.py
│   ├── gen_exploration_notebook.py
│   └── seed_dummy_data.py
│
├── notebooks/
│   └── exploration.ipynb         # EDA notebook (1.5 MB, output-heavy)
│
├── dokumentasi/                  # Project documentation (Indonesian)
│   ├── Design-Dashboard.md
│   ├── Laporan.md
│   ├── Requirement Projek.md
│   ├── Resume-Proyek.md
│   └── dashboard-spec.md
│
├── data/                         # Sample data (empty placeholders)
│   └── sample_reddit.parquet     # 0 bytes
│
├── docker-compose.yml            # All services (Laptop 1 / single machine)
├── docker-compose.laptop2.yml    # Dashboard-only (Laptop 2)
├── .env.example                  # Environment template
├── .env.laptop2.example          # Laptop 2 env template
├── pyproject.toml                # UV project config
├── requirements.txt              # Python dependencies
├── uv.lock                       # UV lock file
├── run_pipeline.sh               # CLI: sentiment | train | deploy
├── main.py                       # Entry stub
├── .python-version               # Python 3.11
└── .gitignore
```

---

## Data Pipeline

### 1. Real-Time OHLC Ingestion

```
Binance WebSocket (btcusdt@trade) ──► Kafka (btc_ticker_raw) ──► Spark Streaming
                                                                       │
                                                                  tumbling window 1m
                                                                       │
                                                                  btc_ohlc_1m (PostgreSQL)
                                                                       │
                                                                  XGBoost inference ──► volatility_pred (PG)
                                                                       │
                                                                  Kafka (volatility_pred topic)
```

- **Binance Producer** (`ingestion/binance_producer.py`): Connects to Binance `@trade` stream, encrypts payloads with Fernet, publishes to Kafka. Exponential backoff with 5 retries + Telegram alert on failure. Malformed messages routed to DLQ.
- **Spark Streaming** (`processing/stream_processor.py`): Reads from Kafka, computes 1-minute tumbling windows (OHLCV + trade_count + volatility), writes to PostgreSQL via PgBouncer with pybreaker circuit breaker. Logs lineage to `pipeline_lineage`.
- **Real-Time XGBoost Inference**: Model loaded from MLflow Registry (cached at `/tmp/xgb_model_cache`). 7 features (rolling_vol_5m, price_range_ratio, vol_ratio, compound_score, positive_ratio, tweet_count, minutes_since_sentiment) → predicted_vol_5m. Falls back to zero if model unavailable.

### 2. Sentiment Analysis

```
Twitter/X scrape ──► MinIO (Parquet) ──► FinVADER scoring ──► sentiment_30m (PG)
```

- **Prefect Flow** (`prefect/flows/sentiment_flow.py`): Scheduled every 30 minutes. Harvests tweets about Bitcoin, stores raw Parquet in MinIO (`twitter-raw/` bucket), runs FinVADER sentiment scoring, validates with Great Expectations, writes aggregated 30-minute windows to PostgreSQL.
- **Quality Flags**: `ok` (>5 tweets), `low_sample` (<5), `stale` (forward-filled on failure).
- **Circuit Breaker**: Pybreaker — stops writes after 5 consecutive failures, auto-resets after 60s.

### 3. ML Training

```
Prefect Flow ──► v_ml_features (PostgreSQL view)
                     │
              7 features + target_vol_5m
                     │
               XGBoost Regressor
               TimeSeriesSplit 5-fold
               StandardScaler
                     │
                MLflow Registry
                (promote if MAE < 0.0015)
```

- **Feature View** (`v_ml_features`): Joins `btc_ohlc_1m` with `sentiment_30m` (forward-filled). Computes rolling_vol_5m, price_range_ratio, vol_ratio, compound_score, positive_ratio, tweet_count, minutes_since_sentiment. Target: `target_vol_5m` (future 5-min volatility).
- **Training Flow** (`prefect/flows/training_flow.py`): Scheduled daily at 23:00 UTC. Extracts features, trains XGBoost (300 trees, max_depth=4, learning_rate=0.05), evaluates with TimeSeriesSplit 5-fold, logs to MLflow. Auto-promotes to Production if MAE < 0.0015 and better than current champion. Sends Telegram alert on failure.

### 4. Federated Query (Trino)

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

- PostgreSQL tables queried directly via `postgresql` catalog.
- MinIO Parquet files queried via `hive` catalog (Hive Metastore tracks schemas).
- Enables JOINs across real-time OHLC + sentiment + raw tweets in a single SQL query.

### 5. Prefect Flows (All)

| Flow | Schedule | Description |
|---|---|---|
| `sentiment-pipeline` | Every 30 min | Twitter scrape → FinVADER → PostgreSQL |
| `model-training` | Daily 23:00 UTC | Feature engineering → XGBoost → MLflow |
| `system-health-check` | Every 60s | Container status, host resources, Telegram alerts |
| `data-quality-check` | Every 60 min | Great Expectations + profiling → data_quality_stats |
| `model-performance-check` | Every 60 min | Production RMSE/MAE (6h sliding window), degradation alerts |
| `log-ingester` | Every 5 min | Aggregate logs → app_logs |

---

## Database Schema

### Core Tables (PostgreSQL — `btcdb`)

| Table | Description | Source |
|---|---|---|
| `btc_ohlc_1m` | OHLC aggregation per 1-minute window | Spark Streaming (Binance) |
| `sentiment_30m` | Twitter sentiment scores per 30-minute window | Prefect (FinVADER) |
| `volatility_pred` | XGBoost real-time predictions with features | Spark Streaming (MLflow model) |
| `btc_predictions` | Predicted vs actual volatility (daily eval) | Prefect (training flow) |
| `pipeline_lineage` | Data lineage: every pipeline run (source, target, rows, status) | All pipelines |
| `audit_log` | Automatic audit trail via INSERT/UPDATE/DELETE triggers | PostgreSQL trigger |

### Governance Tables

| Table | Description |
|---|---|
| `table_metadata` | Central metadata: description, owner, sensitivity, refresh frequency |
| `business_glossary` | Business terms mapped to technical tables/columns |
| `column_lineage` | Source → target column mapping with transformations |
| `data_quality_stats` | Profiling stats: null %, distinct, min/max/mean per column |
| `model_performance` | Historical RMSE/MAE per model version |
| `app_logs` | Aggregated logs from all services |

### Key Relationships

```
btc_ohlc_1m.window_start ──┐
                            ├──► v_ml_features (training view)
sentiment_30m.window_start ─┘
       │                              ┌──► btc_predictions (daily eval)
       └──► volatility_pred ──────────┤
                            XGBoost    └──► Kafka (real-time pred topic)
```

---

## Dashboard

6-page Streamlit dashboard served behind Nginx (HTTPS + Basic Auth):

| Page | Description |
|---|---|
| **Market Overview** | Live OHLC candlestick chart, latest price, volume, 24h change |
| **Volatility Analytics** | Predicted vs actual volatility, feature importances, model MAE trend |
| **Sentiment Analytics** | Compound score timeline, positive/negative ratio, tweet volume |
| **Cross-Source Analytics** | Trino federated queries: join OHLC + sentiment + raw tweets |
| **Data Lineage** | pipeline_lineage table, run history with quality status |
| **Operations** | Data Catalog (metadata, glossary), Data Quality (profiling), System Monitor (containers, CPU/mem/disk), Log Explorer, ML Observability |

Grafana at port 3001 provides additional monitoring with pre-built dashboards and Telegram alerting (4 alert rules).

---

## Service Reference

| Service | Container | Host Port | Credentials |
|---|---|---|---|
| Streamlit Dashboard | `dashboard` | `8501:8501` | (via Nginx) |
| Nginx (HTTPS + Auth) | `nginx` | `8443:443` | `kelompok4_ipbd` / `k4ipbd_nginx_2026` |
| PostgreSQL | `postgres` | `5434:5432` | `kelompok4_ipbd` / `k4ipbd_postgres_2026` |
| PgBouncer | `pgbouncer` | `6432:6432` | (pooled PG, same creds) |
| Trino | `trino` | `8082:8080` | user: `kelompok4_ipbd` |
| Kafka | `kafka` | `9092:9092` | — |
| Kafka UI | `kafka-ui` | `8083:8080` | — |
| MinIO S3 API | `minio` | `9002:9000` | `minioadmin` / `k4ipbd_minio_2026` |
| MinIO Console | `minio` | `9003:9001` | same |
| Spark Master | `spark-master` | `8081:8080` / `7077:7077` | — |
| MLflow | `mlflow` | `5001:5000` | — |
| Prefect Server | `prefect-server` | `4201:4200` | — |
| Grafana | `grafana` | `3001:3000` | `admin` / `k4ipbd_grafana_2026` |
| Marquez API | `marquez-api` | `5002:5000` | — |
| Marquez Web | `marquez-web` | `3002:3000` | — |
| Hive Metastore | `hive-metastore` | `9083:9083` | — |
| Zookeeper | `zookeeper` | `2181:2181` | — |

---

## Security & Governance

### In-Transit Encryption
- Kafka messages encrypted with **Fernet symmetric encryption** before publishing (`ingestion/binance_producer.py`).
- Decryption key sourced from `ENCRYPTION_KEY` env var; consumed by Spark Streaming for inference.

### Access Control
- **Nginx reverse proxy**: HTTPS (self-signed cert) + HTTP Basic Auth for Streamlit dashboard.
- **PostgreSQL roles**: `kelompok4_ipbd` (full admin), `dashboard_reader` (read-only for dashboard), `marquez` (lineage DB owner).
- **PgBouncer**: Transaction pooling with `userlist.txt` authentication (MD5 hash).

### Data Governance
- **Audit Triggers**: Automatic INSERT/UPDATE/DELETE logging to `audit_log` table for all core tables.
- **Data Lineage**: Every pipeline run records source, target, rows_processed, quality_status to `pipeline_lineage`. Marquez collects OpenLineage events for visual lineage.
- **Great Expectations**: 3 validation suites (OHLC, sentiment, predictions) run hourly via Prefect.
- **Data Catalog**: Central `table_metadata` (owner, sensitivity, refresh frequency), `business_glossary`, `column_lineage` (source → target mapping with transformations).
- **Data Quality**: Hourly profiling (null %, distinct, min/max/mean), stored in `data_quality_stats`.

### System Monitoring
- **Telegraf**: Collects CPU, memory, disk, and Docker container metrics every 15s → PostgreSQL.
- **Grafana**: Pre-provisioned dashboards + 4 alert rules (Telegram notifications).
- **Prefect health check**: Every 60s — container status, host resources, anomaly alerts.
- **Model performance check**: Hourly — computes production RMSE/MAE, alerts if degradation > 50%.

---

## Verification

### Check Data Flow (PostgreSQL)

```sql
-- Real-time OHLC data
SELECT COUNT(*) AS rows, MAX(window_start) AS latest
FROM btc_ohlc_1m;

-- Sentiment data
SELECT COUNT(*) AS rows, MAX(window_start) AS latest
FROM sentiment_30m;

-- Predictions
SELECT COUNT(*) AS rows, MAX(window_start) AS latest
FROM volatility_pred;

-- Pipeline lineage
SELECT pipeline_name, quality_status, COUNT(*)
FROM pipeline_lineage
GROUP BY 1, 2
ORDER BY 1;
```

### Federated Query (Trino)

```sql
-- Join PostgreSQL OHLC + MinIO tweets via Trino
SELECT o.window_start, o.close, t.compound
FROM postgresql.public.btc_ohlc_1m o
LEFT JOIN hive.twitter_raw.tweets t
  ON date_trunc('minute', o.window_start) = date_trunc('minute', t.created_at)
LIMIT 10;
```

### Monitoring
- Grafana: `http://host:3001` — check alert rules and pipeline dashboard.
- Prefect: `http://host:4201` — view flow runs, task logs, deployment schedules.
- MLflow: `http://host:5001` — model registry, experiment tracking, feature importance.
- Marquez: `http://host:3002` — visual data lineage graph.
- Kafka UI: `http://host:8083` — topic browser, consumer groups.
