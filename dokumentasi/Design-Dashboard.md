# Detail Design Dashboard — Bitcoin Volatility ML

**Kelompok 4 — IPBD 2026**

---

## 1. Arsitektur Dashboard

```
┌─────────────────┐     ┌──────────────────────┐
│   PostgreSQL    │────►│                      │
│   (btcdb:5434)  │     │    Streamlit App     │     ┌──────────┐
└─────────────────┘     │    (port 8501)       │────►│  Nginx   │────► Browser
                        │                      │     │  :8443   │
┌─────────────────┐     │   dashboard/app.py   │     │ HTTPS +  │
│     Trino       │────►│   + 8 modules        │     │  Auth    │
│   (port 8082)   │     │                      │     └──────────┘
└─────────────────┘     └──────────────────────┘
```

### Konfigurasi Koneksi

| Tujuan | Host | Port | User | Database |
|---|---|---|---|---|
| PostgreSQL (direct) | `localhost` | `5434` | `btcadmin` | `btcdb` |
| Trino (federated) | `localhost` | `8082` | (default) | — |

**Auto-refresh:** 30 detik (`DASHBOARD_REFRESH_SECONDS` di `config.py`)

---

## 2. Struktur File Dashboard

```
dashboard/
├── app.py                # Main app: routing, semua halaman, CSS (~1577 lines)
├── config.py             # Konfigurasi: DB credentials, refresh interval
├── db.py                 # Database helpers: query_pg(), query_trino()
├── system_monitor.py     # Query Telegraf system metrics
├── data_quality.py       # Query data_quality_stats + freshness
├── data_catalog.py       # Query table_metadata, business_glossary, column_lineage
├── ml_observability.py   # Query btc_predictions, model_performance
├── log_explorer.py       # Query app_logs
├── Dockerfile            # Python 3.11-slim + deps
└── requirements.txt      # streamlit, plotly, pandas, psycopg2, trino
```

### Module Dependency

```
app.py
├── from config import DASHBOARD_TITLE, DASHBOARD_REFRESH_SECONDS
├── from db import query_pg, query_trino
├── import system_monitor as smon     → Telegraf metrics tables (cpu, mem, disk, docker_*)
├── import data_quality as dq         → data_quality_stats, pipeline_lineage
├── import data_catalog as dc         → table_metadata, business_glossary, column_lineage, information_schema
├── import ml_observability as mlo    → btc_predictions, model_performance
└── import log_explorer as logexp     → app_logs
```

---

## 3. Design System

### Palet Warna

| Token | Hex | Penggunaan |
|---|---|---|
| Background | `#0e1117` | Page background (dark) |
| Card | `#1a1d23` | Card background |
| Border | `#2d3139` | Card borders, gridlines |
| Primary/Accent | `#f7931a` | Bitcoin orange — KPI values, chart lines, buttons |
| Success | `#00d4aa` | Positive sentiment, running status, OK, passed |
| Danger | `#ff4b4b` | Negative sentiment, errors, exited, failed |
| Warning | `#ffc107` | Neutral/warning states |
| Muted Text | `#9ba3af` | Labels, descriptions, secondary text |
| Muted Dark | `#5b616e` | Tertiary text, footers |

### Tipografi

| Level | Size | Weight | Usage |
|---|---|---|---|
| H1 | 1.6rem | 600 | Page title |
| H2 | 1.2rem | 600 | Section headers |
| KPI Value | 1.5rem / 1.1rem | 700 | Card values (large / compact) |
| KPI Label | 0.75rem / 0.7rem | 400 | Card labels (uppercase) |
| Delta | 0.85rem / 0.75rem | 400 | Change indicators |
| Body | 0.85rem | 400 | Tables, general text |

### CSS Classes

| Class | Padding | Radius | Purpose |
|---|---|---|---|
| `.card` | 1rem 1.2rem | 8px | Large card (unused, dead code) |
| `.card-compact` | 0.6rem 1rem | 6px | KPI metric cards (all pages) |
| `.sidebar-section` | border-bottom | — | Sidebar divider |

---

## 4. Sidebar

```
┌─────────────────────────────┐
│ BITCOIN VOLATILITY ML       │  ← label muted
│ HH:MM:SS UTC                │  ← waktu real-time
├─────────────────────────────┤
│ [ Refresh ]                 │  ← tombol st.button, full-width, orange hover
├─────────────────────────────┤
│ ○ Summary                   │
│ ○ Market Overview           │  ← st.radio (label hidden)
│ ○ Volatility Analytics      │
│ ○ Sentiment Analytics       │
│ ○ Pipeline Operations       │
│ ○ Operations                │
├─────────────────────────────┤
│ PostgreSQL — real-time data │
│ Trino — cross-source        │  ← footer info
│ Kelompok 4 — IPBD 2026      │
└─────────────────────────────┘
```

---

## 5. Halaman Detail

### 5.1 Summary (Home)

**Tujuan:** Gambaran umum sistem secara ringkas.

**Data Sources:** `btc_ohlc_1m`, `sentiment_30m`, `volatility_pred`, `app_logs`, Telegraf metrics

| Row | Layout | Konten | Sumber Data |
|---|---|---|---|
| 1 | `st.columns(4)` | **KPI 1:** BTC/USDT price + 24h change %. **KPI 2:** Sentiment score (color-coded hijau/kuning/merah). **KPI 3:** Predictions count (1 jam). **KPI 4:** System health (running/total containers) | `btc_ohlc_1m`, `sentiment_30m`, `volatility_pred`, Docker socket |
| 2 | `st.columns(3)` | **Mini Chart 1:** BTC price line 2 jam terakhir (orange fill). **Mini Chart 2:** CPU usage gauge (0-100%). **Mini Chart 3:** Model Performance card (MAE, RMSE, n, version) | `btc_ohlc_1m`, Telegraf `cpu`, `model_performance` |
| 3 | Full width | **Recent Alerts:** 5 log ERROR/CRITICAL/WARNING terbaru, border-left color-coded | `app_logs` |

**Chart Types:** Plotly `go.Scatter` (line fill), `go.Indicator` (gauge), HTML cards

---

### 5.2 Market Overview

**Tujuan:** Analisis pergerakan harga Bitcoin 24 jam terakhir.

**Data Sources:** `btc_ohlc_1m`

| Row | Layout | Konten | Sumber Data |
|---|---|---|---|
| 1 | `st.columns(6)` | **6 KPI:** Harga BTC/USDT + delta, Volatilitas terbaru, Volume 24h, Trade count 24h, Sentimen terbaru, Predictions 1h | `btc_ohlc_1m`, `sentiment_30m`, `volatility_pred` |
| 2 | `st.columns([2, 1])` | **Kiri:** Candlestick chart (OHLC 24h, green/red). **Kanan:** Volume bar chart per jam (orange gradient) | `btc_ohlc_1m` |
| 3 | `st.columns(2)` | **Kiri:** Volatility timeseries line (orange fill). **Kanan:** Price histogram distribusi (30 bins) | `btc_ohlc_1m` |

**Chart Types:** `go.Candlestick`, `px.bar`, `go.Scatter` (fill), `px.histogram`

---

### 5.3 Volatility Analytics

**Tujuan:** Analisis prediksi volatilitas, performa model XGBoost, dan drill-down metrik.

**Data Sources:** `volatility_pred`, `btc_ohlc_1m`, `btc_predictions`, `model_performance`

| Row | Layout | Konten | Sumber Data |
|---|---|---|---|
| 1 | `st.columns(4)` | **4 KPI:** Mean Pred Vol (7d), Max Volatility (24h), Model Version, Total Inferences (7d) | `volatility_pred` |
| 2 | `st.columns(2)` | **Kiri:** Predicted volatility timeseries 7 hari. **Kanan:** Feature means bar chart horizontal (rolling_vol_5m, price_range_ratio, vol_ratio) | `volatility_pred` |
| 3 | `st.columns(2)` | **Kiri:** Volume vs Volatility scatter plot (size = trade_count). **Kanan:** Box plot volatilitas per jam UTC | `btc_ohlc_1m` |
| 4 | `st.columns(2)` | **Kiri:** Predicted vs Actual overlay line chart (48h). **Kanan:** Residual histogram (Actual - Predicted, 30 bins, zero-line) | `btc_predictions` |
| 5 | `st.columns(3)` + full width | **Cards:** Production MAE, RMSE, N Predictions. **Chart:** MAE + RMSE history timeseries | `model_performance` |

**Chart Types:** `go.Scatter`, `px.bar` (horizontal), `px.scatter`, `px.box`, `px.histogram`

---

### 5.4 Sentiment Analytics

**Tujuan:** Analisis data sentimen Twitter dan korelasi dengan harga BTC.

**Data Sources:** `sentiment_30m`, Trino (federated: PG + Hive/MinIO)

| Row | Layout | Konten | Sumber Data |
|---|---|---|---|
| 1 | `st.columns(4)` | **4 KPI:** Compound Score (color-coded), Positive Ratio, Total Tweets 7d, Data Quality flag | `sentiment_30m` |
| 2 | `st.columns(2)` | **Kiri:** Compound score timeseries 7d (±0.05 threshold lines). **Kanan:** Donut chart ratio (Positive/Negative/Neutral) | `sentiment_30m` |
| 3 | `st.columns(2)` | **Kiri:** Tweet volume bar chart per 30min window (blue gradient). **Kanan:** Weighted vs Unweighted compound comparison line chart | `sentiment_30m` |
| 4 | Full width | **Federated Query (Trino):** Tombol "Jalankan Query Federasi" → JOIN `postgresql.public.btc_ohlc_1m` ⨯ `hive.twitter_raw.tweets`. Tampilkan: dataframe, scatter plot + regression line (sentiment vs price), correlation coefficient. **SQL Explorer:** Text area + tombol untuk custom Trino SQL | Trino |

**Chart Types:** `go.Scatter` (fill + threshold lines), `go.Pie` (donut), `px.bar`, `px.scatter` (with trendline)

---

### 5.5 Pipeline Operations

**Tujuan:** Monitoring eksekusi pipeline Proses dan audit trail database.

**Data Sources:** `pipeline_lineage`, `audit_log`, `volatility_pred`

| Row | Layout | Konten | Sumber Data |
|---|---|---|---|
| 1 | `st.columns(4)` | **4 KPI:** Total Rows Processed, Success Rate %, Number of Pipelines, Last Run (time ago) | `pipeline_lineage` |
| 2 | `st.columns(2)` | **Kiri:** Bar chart: rows per pipeline. **Kanan:** Donut: quality status distribution (ok/failed/promoted/stale) | `pipeline_lineage` |

#### Tab: Pipeline Lineage

| Row | Layout | Konten |
|---|---|---|
| 3 | `st.columns(2)` | **Kiri:** Timeline Gantt chart (started_at → finished_at per pipeline). **Kanan:** Inference latency timeseries (ms) |
| 4 | Full width | Table lineage 100 run terakhir + CSV download |

**Chart Types:** `px.timeline`, `go.Scatter` (latency), `px.bar`

#### Tab: Audit Log

| Row | Layout | Konten |
|---|---|---|
| 3 | Full width | Table: last 100 audit records (table_name, operation, changed_by, changed_at) + CSV download |
| 4 | Full width | Operation distribution donut (INSERT/UPDATE/DELETE) |

**Chart Types:** `go.Pie` (donut)

---

### 5.6 Operations (4 Sub-tab)

#### 5.6a System Health

**Tujuan:** Monitoring infrastruktur: host & container resources.

**Data Sources:** Telegraf metrics tables (`cpu`, `mem`, `disk`, `docker`, `docker_container_cpu`, `docker_container_mem`)

| Row | Layout | Konten |
|---|---|---|
| 1 | `st.columns(4)` | **4 KPI:** Host CPU % (color-coded threshold), Host Memory (used/total GB), Host Disk (used/total GB), Containers (running/total + stopped count) |
| 2 | `st.columns(2)` | **Kiri:** Host CPU usage timeseries (threshold lines 80%/90%). **Kanan:** Host Memory % timeseries |
| 3 | `st.columns([3, 2])` | **Kiri:** Container Status Table (running ✅/exited ❌ + memory %). **Kanan:** 3 Gauges: CPU %, Memory %, Disk % (stacked vertikal) |
| 4 | `st.columns(2)` | **Kiri:** Container CPU timeseries (top 5 containers). **Kanan:** Container Memory timeseries (top 5 containers) |

**Chart Types:** `go.Scatter`, `go.Indicator` (gauge), `st.dataframe`

#### 5.6b Data Quality

**Tujuan:** Profiling kualitas data per tabel per kolom.

**Data Sources:** `data_quality_stats`, `pipeline_lineage`

| Row | Layout | Konten |
|---|---|---|
| 1 | `st.columns(4)` | **4 KPI:** Completeness % (color-coded), Tables Profiled, Last Validation (OK/FAILED), Type Errors count |
| 2 | `st.columns(2)` | **Kiri:** Null % Heatmap (imshow, rows=tables, cols=columns, green→red). **Kanan:** Data Freshness table (last data timestamp + age + icon) |
| 3 | Full width | Column Profiling Stats table: nulls, distinct, min/max/mean, type errors per column + CSV download |
| 4 | `st.columns([2, 1])` | **Kiri:** Validation history scatter timeline (ok/failed). **Kanan:** Passed/Failed donut + summary text |

**Chart Types:** `px.imshow` (heatmap), `px.scatter`, `go.Pie` (donut), `st.dataframe`

#### 5.6c Data Catalog

**Tujuan:** Eksplorasi metadata, lineage, dan business glossary.

**Data Sources:** `table_metadata`, `business_glossary`, `column_lineage`, PostgreSQL `information_schema.columns`

| Row | Layout | Konten |
|---|---|---|
| 1 | Full width | **Sankey Diagram:** Visual data lineage flow (all source → target edges) |
| 2 | `st.columns([1, 2])` | **Kiri:** Table metadata (deskripsi, owner, sensitivity, refresh, source, retention) untuk tabel yang dipilih. **Kanan:** Column info table (name, data_type, nullable, comment) |
| 3 | `st.columns(2)` | **Kiri:** Upstream column lineage (source → target). **Kanan:** Downstream column lineage (source → target) |
| 4 | Full width | Business Glossary table (term, definition, technical table/column, category) + CSV download |

**Chart Types:** `go.Sankey`, `st.selectbox`, `st.dataframe`

#### 5.6d Logs

**Tujuan:** Eksplorasi dan analisis application logs.

**Data Sources:** `app_logs`

| Row | Layout | Konten |
|---|---|---|
| 0 | `st.columns(2)` | **Filter:** Service dropdown + Level dropdown |
| 1 | `st.columns(4)` | **3 KPI:** Total Logs, Errors (color-coded if > 0), Warnings. Kolom ke-4 kosong (wasted) |
| 2 | `st.columns([3, 1])` | **Kiri:** Log table (timestamp, service, level, message, run_id) + CSV download, height=400. **Kanan:** Donut log level distribution |
| 3 | `st.columns(2)` | **Kiri:** Error timeseries bar chart (per hour). **Kanan:** Pipeline error summary table |
| 4 | Full width | Recent Alerts table (30 record) + CSV download |

**Chart Types:** `go.Pie` (donut), `px.bar`, `st.dataframe`

---

## 6. Data Flow per Halaman

```
Page Summary
  btc_ohlc_1m ──────► Harga BTC, Mini chart line
  sentiment_30m ─────► Sentimen KPI
  volatility_pred ───► Predictions count
  app_logs ──────────► Recent alerts
  cpu ───────────────► CPU gauge
  model_performance ─► Model perf card

Page Market Overview
  btc_ohlc_1m ──────► Semua chart & KPI

Page Volatility Analytics
  volatility_pred ───► Predicted vol, features, model version, total inferences
  btc_ohlc_1m ───────► Scatter (volume vs vol), box plot (vol by hour)
  btc_predictions ───► Predicted vs Actual, residuals
  model_performance ─► Perf metrics, MAE/RMSE history

Page Sentiment Analytics
  sentiment_30m ─────► Semua chart & KPI
  Trino (PG+Hive) ───► Federated query, scatter, korelasi

Page Pipeline Operations
  pipeline_lineage ──► Timeline, table, stats, donut
  audit_log ─────────► Audit table, donut
  volatility_pred ───► Inference latency

Page Operations
  System Health:
    cpu, mem, disk, docker, docker_container_cpu, docker_container_mem (Telegraf)
  Data Quality:
    data_quality_stats, pipeline_lineage (DQ check entries)
  Data Catalog:
    table_metadata, business_glossary, column_lineage, information_schema.columns
  Logs:
    app_logs
```

---

## 7. Known Issues & Rencana Perbaikan

| # | Issue | Lokasi | Severity | Rencana Fix |
|---|---|---|---|---|
| 1 | `st.metric` + `card-compact` campur di Summary Row 1 menyebabkan tinggi card tidak rata | `app.py:145-170` | Medium | Samakan semua jadi `card-compact` HTML |
| 2 | `st.columns(6)` di Market Overview terlalu sempit, teks wrap di layar <1366px | `app.py:278` | High | Pecah jadi 2 baris `st.columns(3)` |
| 3 | 3 gauge bertumpuk vertikal 660px di System Health, tidak seimbang dengan kiri 400px | `app.py:1106-1164` | Medium | Sub-kolom `st.columns(3)` dalam `col_r2` |
| 4 | Auto-refresh `sleep(30)` freeze tanpa indikator, blinks on rerun | `app.py:1574-1577` | Medium | Tambahkan countdown text "Next refresh in Xs" |
| 5 | Chart heights tidak konsisten di paired columns — beda tinggi kiri-kanan | ~20 lokasi | Medium | Tambahkan `height=300` ke semua chart dalam `st.columns(2)` |
| 6 | CSS class `.card` didefinisikan tapi tidak dipakai | `app.py:28-34` | Low | Hapus 7 baris dead CSS |
| 7 | Kolom `c4` di Logs KPI row tidak terpakai (25% kosong) | `app.py:1494` | Low | Ubah ke `st.columns(3)` |
| 8 | Redundant `import numpy as np` di dalam fungsi | `app.py:1253` | Trivial | Hapus baris tersebut |
| 9 | `st.columns([2,2,2])` verbositas tidak perlu | `app.py:173` | Trivial | Simplifikasi ke `st.columns(3)` |
