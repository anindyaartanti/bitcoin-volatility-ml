# Dashboard Specification — Bitcoin Volatility ML

Kelompok 4 — IPBD 2026

---

## 1. Architecture

```
┌─────────────────┐     ┌──────────────────────┐
│   PostgreSQL    │────►│                      │
│   (btcdb:5434)  │     │    Streamlit App     │
└─────────────────┘     │    (port 8501)       │     ┌──────────┐
                        │                      │────►│  Nginx   │────► Browser
┌─────────────────┐     │   dashboard/app.py   │     │  :8443   │
│     Trino       │────►│   + 8 modules        │     │ HTTPS +  │
│   (port 8082)   │     │                      │     │  Auth    │
└─────────────────┘     └──────────────────────┘     └──────────┘
```

**Database connections:**

| Target | Host | Port | User | Database |
|---|---|---|---|---|
| PostgreSQL (direct) | `localhost` | `5434` | `btcadmin` | `btcdb` |
| Trino (federated) | `localhost` | `8082` | (default) | — |

**Auto-refresh:** 30 seconds

**Module structure:**
```
dashboard/
├── app.py                # Main app: routing, all pages, CSS
├── config.py             # Config: DB credentials, refresh interval
├── db.py                 # Database helpers: query_pg(), query_trino()
├── system_monitor.py     # Query Telegraf system metrics
├── data_quality.py       # Query data_quality_stats + freshness
├── data_catalog.py       # Query table_metadata, business_glossary, column_lineage
├── ml_observability.py   # Query btc_predictions, model_performance
├── log_explorer.py       # Query app_logs
├── Dockerfile
└── requirements.txt
```

---

## 2. Design System

### Color Palette

| Token | Hex | Usage |
|---|---|---|
| Background | `#0e1117` | Page background (dark theme) |
| Card | `#1a1d23` | Card background |
| Border | `#2d3139` | Card borders, gridlines |
| Primary / Accent | `#f7931a` | Bitcoin orange — KPI values, chart lines, buttons |
| Success | `#00d4aa` | Positive sentiment, running, OK, passed |
| Danger | `#ff4b4b` | Negative sentiment, errors, exited, failed |
| Warning | `#ffc107` | Neutral/warning states |
| Muted Text | `#9ba3af` | Labels, descriptions, secondary text |
| Muted Dark | `#5b616e` | Tertiary text, footers |

### Typography

| Level | Size | Weight | Usage |
|---|---|---|---|
| H1 | 1.6rem | 600 | Page title |
| H2 | 1.2rem | 600 | Section headers |
| KPI Value | 1.1rem | 700 | Card compact values |
| KPI Label | 0.7rem | 400 | Card labels (uppercase) |
| Delta | 0.75rem | 400 | Change indicators |
| Body | 0.85rem | 400 | Tables, general text |

### CSS Classes

| Class | Padding | Border-radius | Purpose |
|---|---|---|---|
| `.card-compact` | 0.6rem 1rem | 6px | KPI metric cards on all pages |

### Chart Theme Helper

All Plotly charts use a shared theming function:
- `paper_bgcolor`: `#0f131b`
- `plot_bgcolor`: `#0f131b`
- `font_color`: `#9ba3af`
- Grid: y-axis only, color `#2d3139`
- Margins: 10px all sides
- Default height: 280px
- Hover mode: `x unified`

---

## 3. Sidebar Navigation

Top-to-bottom layout:

1. **Header**: "BITCOIN VOLATILITY ML" muted label + current time in UTC
2. **Refresh** button (full-width, orange hover)
3. **Navigation radio** (label hidden, 6 items):
   - Summary
   - Market Overview
   - Volatility Analytics
   - Sentiment Analytics
   - Pipeline Operations
   - Operations
4. **Divider**
5. **Footer**: "PostgreSQL — real-time data", "Trino — cross-source analytics", "Kelompok 4 — IPBD 2026"

---

## 4. Page 1 — Summary (Home)

**Purpose:** Brief overview of the entire system at a glance.

### 4.1 Row 1 — 4 KPI Cards

| Position | Label | Value | Data Source | Computation |
|---|---|---|---|---|
| Col 1 | BTC/USDT | `$xx,xxx.xx` | `btc_ohlc_1m` 24h | `close.iloc[-1]`, delta = `(last / first - 1) * 100` |
| Col 2 | Sentiment | `±x.xxxx` (color-coded) | `sentiment_30m` latest | `compound_score` — green if >0.05, yellow if >-0.05, red otherwise |
| Col 3 | Predictions (1h) | `N` | `volatility_pred` 1h | `COUNT(*)` |
| Col 4 | System | `{running}/{total} Up` (color-coded) | `docker_container_status` | green if all running, yellow otherwise |

```sql
-- BTC price
SELECT window_start, close FROM btc_ohlc_1m
WHERE window_start > NOW() - INTERVAL '24 hours'
ORDER BY window_start;

-- Sentiment
SELECT compound_score FROM sentiment_30m
ORDER BY window_start DESC LIMIT 1;

-- Predictions count
SELECT COUNT(*) AS cnt FROM volatility_pred
WHERE window_start > NOW() - INTERVAL '1 hour';

-- Recent alerts
SELECT timestamp, service, level, message FROM app_logs
WHERE level IN ('ERROR','CRITICAL','WARNING')
ORDER BY timestamp DESC LIMIT 5;
```

### 4.2 Row 2 — 3 Mini Panels

| Panel | Content | Data Source |
|---|---|---|
| Left | BTC price line chart (last 2h, orange fill) | `btc_ohlc_1m` `.tail(120)` rows → `window_start, close` |
| Middle | CPU usage gauge (0-100%) | `cpu` table, latest row: `100 - usage_idle` |
| Right | Model performance card (MAE, RMSE, n, version) | `model_performance` latest row |

### 4.3 Row 3 — Recent Alerts

List of last 5 app log entries (ERROR/CRITICAL/WARNING), each with:
- Color-coded left border (red for ERROR/CRITICAL, yellow for WARNING)
- Icon: 🔴 or 🟡
- Format: `[HH:MM] service: message`

---

## 5. Page 2 — Market Overview

**Purpose:** Bitcoin price and volume analysis, last 24 hours.

### 5.1 Row 1 — 6 KPI Cards (2 rows of 3)

| KPI | Value | Source |
|---|---|---|
| BTC/USDT | `$xx,xxx.xx` + `±x.xx%` delta | `btc_ohlc_1m` 24h: last close vs first close |
| Volatility | `x.xxxx` (latest) | `btc_ohlc_1m` last row |
| Volume 24h | `xxx,xxx` (sum) | `btc_ohlc_1m` 24h: `SUM(volume)` |
| Trades 24h | `xxx,xxx` (sum) | `btc_ohlc_1m` 24h: `SUM(trade_count)` |
| Sentiment | `±x.xxx` (color-coded) | `sentiment_30m` latest |
| Predictions 1h | `N` (count) | `volatility_pred` 1h: `COUNT(*)` |

```sql
-- OHLC 24h
SELECT window_start, open, high, low, close, volume, trade_count, volatility
FROM btc_ohlc_1m
WHERE window_start > NOW() - INTERVAL '24 hours'
ORDER BY window_start;

-- Hourly aggregation
SELECT date_trunc('hour', window_start) AS hour,
       AVG(close) AS avg_close,
       SUM(volume) AS total_volume,
       COUNT(*) AS trade_count
FROM btc_ohlc_1m
WHERE window_start > NOW() - INTERVAL '24 hours'
GROUP BY 1 ORDER BY 1;
```

### 5.2 Row 2 — Candlestick + Volume Bar

| Left (⅔) | Right (⅓) |
|---|---|
| Plotly Candlestick chart: OHLC 24h, green (`#00d4aa`) for up / red (`#ff4b4b`) for down | Plotly Bar chart: volume per hour, orange color scale (`Oranges`) |

### 5.3 Row 3 — Volatility Timeseries + Price Histogram

| Left (½) | Right (½) |
|---|---|
| Line chart: `window_start` vs `volatility`, orange line + fill, 24h | Histogram: `close` distribution, 30 bins, orange |

---

## 6. Page 3 — Volatility Analytics

**Purpose:** Volatility prediction analysis, model evaluation, feature drill-down.

### 6.1 Row 1 — 4 KPI Cards

| KPI | Value | Source |
|---|---|---|
| Mean Pred Vol 7d | `x.xxxxxx` | `AVG(predicted_vol_5m)` from `volatility_pred` 7d |
| Max Volatility 24h | `x.xxxx` | `MAX(volatility)` from `btc_ohlc_1m` 24h where NOT NULL |
| Model Version | `vX` | latest `model_version` from `volatility_pred` GROUP BY |
| Inferences 7d | `N` | `COUNT(*)` from `volatility_pred` 7d |

```sql
-- Predictions 7d
SELECT window_start, predicted_vol_5m, rolling_vol_5m,
       price_range_ratio, vol_ratio, inference_latency_ms
FROM volatility_pred
WHERE window_start > NOW() - INTERVAL '7 days'
ORDER BY window_start;

-- Feature means (UNION ALL)
SELECT 'rolling_vol_5m' AS feature, AVG(rolling_vol_5m) AS mean_val FROM volatility_pred
UNION ALL SELECT 'price_range_ratio', AVG(price_range_ratio) FROM volatility_pred
UNION ALL SELECT 'vol_ratio', AVG(vol_ratio) FROM volatility_pred;

-- Volume-volatility scatter data
SELECT volume, volatility, trade_count FROM btc_ohlc_1m
WHERE window_start > NOW() - INTERVAL '24 hours' AND volatility IS NOT NULL;

-- Volatility by hour (box plot)
SELECT EXTRACT(HOUR FROM window_start) AS hour, volatility
FROM btc_ohlc_1m
WHERE window_start > NOW() - INTERVAL '7 days' AND volatility IS NOT NULL;

-- Model versions
SELECT model_version, COUNT(*) AS n_preds,
       MAX(window_start) AS last_seen
FROM volatility_pred
WHERE model_version IS NOT NULL
GROUP BY model_version ORDER BY last_seen DESC;
```

### 6.2 Row 2 — Predicted Vol Timeseries + Feature Means

| Left (½) | Right (½) |
|---|---|
| Line chart: `predicted_vol_5m` over time, 7 days, orange line | Horizontal bar chart: 3 features (rolling_vol_5m, price_range_ratio, vol_ratio), orange color scale |

### 6.3 Row 3 — Volume-Volatility Scatter + Box Plot

| Left (½) | Right (½) |
|---|---|
| Scatter: `volume` vs `volatility`, size by `trade_count`, color by `volatility` (Oranges) | Box plot: volatility per hour UTC, orange markers |

### 6.4 Row 4 — Predicted vs Actual + Residuals

Section header: "Model Prediction Accuracy"

| Left (½) | Right (½) |
|---|---|
| Dual-line chart: predicted (orange solid) vs actual (green dashed dot) over 48h | Residual histogram: 30 bins, zero-line in red dash |
| Data from `btc_predictions` WHERE `actual_vol IS NOT NULL` | Residual computed client-side: `actual_vol - predicted_vol` |

```sql
SELECT window_start, predicted_vol, actual_vol, model_version
FROM btc_predictions
WHERE window_start > NOW() - INTERVAL '48 hours'
  AND actual_vol IS NOT NULL
ORDER BY window_start;
```

### 6.5 Row 5 — Model Performance Metrics

3 KPI cards: Production MAE, Production RMSE, N Predictions (from `model_performance` latest row)

Performance history line chart: MAE (orange) + RMSE (red) over time, last 50 records.

```sql
SELECT window_end AS checked_at, model_version, rmse, mae, n_predictions
FROM model_performance
ORDER BY checked_at DESC LIMIT 50;
```

---

## 7. Page 4 — Sentiment Analytics

**Purpose:** Twitter/X sentiment analysis and correlation with BTC price.

### 7.1 Row 1 — 4 KPI Cards

| KPI | Value | Source |
|---|---|---|
| Compound Score | `±x.xxx` (color-coded) | `sentiment_30m` latest `compound_score` |
| Positive Ratio | `x.xxx` | `sentiment_30m` latest `positive_ratio` |
| Tweets 7d | `N` | `sentiment_30m` 7d: `SUM(tweet_count)` |
| Data Quality | `ok` / `low_sample` / `stale` | `sentiment_30m` latest `data_quality` |

```sql
SELECT window_start, compound_score, positive_ratio, negative_ratio,
       neutral_ratio, weighted_compound, tweet_count, data_quality
FROM sentiment_30m
WHERE window_start > NOW() - INTERVAL '7 days'
ORDER BY window_start;
```

### 7.2 Row 2 — Compound Timeseries + Sentiment Donut

| Left (½) | Right (½) |
|---|---|
| Line chart: `compound_score` over time, 7 days, with threshold lines at +0.05 (green dash) and -0.05 (red dash) | Donut chart: positive_ratio, negative_ratio, neutral_ratio, colors: green / red / yellow |

### 7.3 Row 3 — Tweet Volume + Weighted vs Unweighted

| Left (½) | Right (½) |
|---|---|
| Bar chart: `tweet_count` per 30-minute window, blue gradient | Dual line chart: `compound_score` (orange) vs `weighted_compound` (green dashed) |

### 7.4 Row 4 — Federated Query (Trino)

**Button**: "Jalankan Query Federasi"

Federated SQL joins PostgreSQL OHLC with Hive/MinIO tweets via Trino:

```sql
SELECT
    o.window_start,
    o.close,
    o.volatility,
    COALESCE(t.avg_compound, 0) AS avg_tweet_sentiment,
    COALESCE(t.tweet_count, 0) AS tweet_count
FROM postgresql.public.btc_ohlc_1m o
LEFT JOIN (
    SELECT
        date_trunc('minute', created_at) AS tweet_minute,
        AVG(compound) AS avg_compound,
        COUNT(*) AS tweet_count
    FROM hive.twitter_raw.tweets
    WHERE created_at > TIMESTAMP 'UTC' - INTERVAL '1' HOUR
    GROUP BY 1
) t ON o.window_start = t.tweet_minute
WHERE o.window_start > TIMESTAMP 'UTC' - INTERVAL '1' HOUR
ORDER BY o.window_start;
```

Output:
- DataFrame table of results
- Scatter plot: `close` vs `avg_tweet_sentiment`, size by `tweet_count`
- OLS regression trendline (computed in Python with `numpy.polyfit`)
- Pearson correlation coefficient (computed in Python)

**SQL Explorer:** text area for custom Trino SQL queries with execute button.

---

## 8. Page 5 — Pipeline Operations

**Purpose:** Pipeline execution monitoring and audit trail.

### 8.1 Row 1 — 4 KPI Cards

| KPI | Value | Source | SQL |
|---|---|---|---|
| Total Rows | `xxx,xxx` | `pipeline_lineage` | `SUM(rows_processed)` |
| Success Rate | `xx.x%` | `pipeline_lineage` | `AVG(CASE WHEN quality_status='ok' THEN 1 ELSE 0 END) * 100` |
| Pipelines | `N` | `pipeline_lineage` | `COUNT(DISTINCT pipeline_name)` |
| Last Run | `Xh ago` | `pipeline_lineage` | `MAX(finished_at)`, age computed client-side |

### 8.2 Row 2 — Rows per Pipeline + Quality Donut

| Left (½) | Right (½) |
|---|---|
| Bar chart: `SUM(rows_processed)` grouped by `pipeline_name` | Donut: quality_status distribution (ok, failed, promoted, stale) |

### 8.3 Tab: Pipeline Lineage

| Row | Content |
|---|---|
| Row 3 col left | Timeline Gantt chart: `started_at` → `finished_at` per pipeline run (Plotly `px.timeline`) |
| Row 3 col right | Inference latency timeseries: `inference_latency_ms` from `volatility_pred` |
| Row 4 full width | Table of last 100 pipeline runs + CSV download |

### 8.4 Tab: Audit Log

| Row | Content |
|---|---|
| Row 3 full width | Table: last 100 audit records (`table_name`, `operation`, `changed_by`, `changed_at`) + CSV download |
| Row 4 full width | Donut chart: operation distribution (INSERT/UPDATE/DELETE) |

```sql
SELECT table_name, operation, changed_by, changed_at
FROM audit_log ORDER BY changed_at DESC LIMIT 100;
```

---

## 9. Page 6 — Operations

4 sub-tabs: System Health, Data Quality, Data Catalog, Logs.

---

### 9.1 Sub-tab: System Health

**Purpose:** Infrastructure monitoring — host & container resources.

**Source:** Telegraf metrics tables in PostgreSQL (`cpu`, `mem`, `disk`, `docker_container_cpu`, `docker_container_mem`, `docker_container_status`).

These tables are auto-created by Telegraf on first metric flush. If tables don't exist, show a warning and skip.

#### 9.1.1 Row 1 — 4 KPI Cards

| KPI | Value | Source | Computation |
|---|---|---|---|
| Host CPU | `xx.x%` (color-coded) | `cpu` latest | `100 - usage_idle`, threshold: green <80%, yellow <90%, red >90% |
| Host Memory | `X.X / X.X GB` | `mem` latest | `used` and `total` in GB |
| Host Disk | `X.X / X.X GB` | `disk` latest (exclude `/var/lib/docker/*`) | `used` and `total` in GB |
| Containers | `{running}/{total} Running` | `docker_container_status` 5min | count running vs total, show stopped count |

```sql
-- Host CPU 24h
SELECT time, usage_idle, usage_system, usage_user
FROM cpu WHERE time > NOW() - INTERVAL '24 hours' ORDER BY time;

-- Host Memory 24h
SELECT time, total, available, used, used_percent
FROM mem WHERE time > NOW() - INTERVAL '24 hours' ORDER BY time;

-- Host Disk 24h
SELECT time, path, total, used, used_percent
FROM disk WHERE time > NOW() - INTERVAL '24 hours' ORDER BY time;

-- Container CPU 15min
SELECT time, container_name, usage_percent
FROM docker_container_cpu
WHERE time > NOW() - INTERVAL '15 minutes'
  AND container_name IS NOT NULL ORDER BY time;

-- Container Memory latest
SELECT DISTINCT ON (container_name)
    container_name, usage_percent, usage, "limit", time
FROM docker_container_mem
WHERE time > NOW() - INTERVAL '5 minutes' AND container_name IS NOT NULL
ORDER BY container_name, time DESC;

-- Container Status latest
SELECT DISTINCT ON (container_name)
    container_name, status, oomkilled, time
FROM docker_container_status
WHERE time > NOW() - INTERVAL '5 minutes' AND container_name IS NOT NULL
ORDER BY container_name, time DESC;
```

#### 9.1.2 Row 2 — Host CPU + Memory Timeseries

| Left (½) | Right (½) |
|---|---|
| Line chart: `100 - usage_idle` over 24h, 80% threshold (yellow dash), 90% threshold (red dash) | Line chart: `used_percent` over 24h, 85% threshold (red dash) |

#### 9.1.3 Row 3 — Container Table + Gauges

| Left (3/5) | Right (2/5) |
|---|---|
| Table: container name, status icon (✅ running, ❌ exited, 🔄 other), memory % | 3 stacked gauges: CPU %, Memory %, Disk %. Green <80%, yellow <90%, red >90% |

#### 9.1.4 Row 4 — Container CPU/Memory Trends

| Left (½) | Right (½) |
|---|---|
| Multi-line chart: container CPU % over 15min, top 5 containers | Multi-line chart: container memory % over 15min, top 5 containers |

---

### 9.2 Sub-tab: Data Quality

**Purpose:** Data profiling per table per column.

**Source:** `data_quality_stats`, `pipeline_lineage`.

#### 9.2.1 Row 1 — 4 KPI Cards

| KPI | Value | Source | Computation |
|---|---|---|---|
| Completeness | `xx.x%` (color-coded) | `data_quality_stats` | `AVG(100 - null_percent)`, green >95%, yellow >85%, red otherwise |
| Tables Profiled | `N` | `data_quality_stats` | `COUNT(DISTINCT table_name)` |
| Last Validation | `OK` / `FAILED` | `pipeline_lineage` WHERE `pipeline_name='data_quality_check'` | latest `quality_status` |
| Type Errors | `N` | `data_quality_stats` | `SUM(type_mismatch)` |

```sql
-- Latest DQ stats
SELECT DISTINCT ON (table_name, column_name)
    table_name, column_name, null_count, total_rows, null_percent,
    distinct_count, type_mismatch, min_value, max_value, mean_value,
    quality_status, checked_at
FROM data_quality_stats
ORDER BY table_name, column_name, checked_at DESC;

-- DQ summary per table
WITH latest AS (
    SELECT DISTINCT ON (table_name, column_name)
        table_name, column_name, null_percent, distinct_count,
        total_rows, quality_status, checked_at
    FROM data_quality_stats
    ORDER BY table_name, column_name, checked_at DESC
)
SELECT table_name,
       ROUND(AVG(100.0 - COALESCE(null_percent, 0)), 1) AS completeness_pct,
       COUNT(*) AS columns_checked,
       MAX(checked_at) AS last_checked,
       COUNT(*) FILTER (WHERE quality_status != 'ok') AS failed_columns,
       SUM(COALESCE(type_mismatch, 0)) AS total_type_errors
FROM latest GROUP BY table_name ORDER BY table_name;

-- Validation history
SELECT started_at, quality_status, params->>'table_name' AS table_name,
       params->>'checks_passed' AS checks_passed,
       params->>'checks_failed' AS checks_failed
FROM pipeline_lineage
WHERE pipeline_name = 'data_quality_check'
ORDER BY started_at DESC LIMIT 50;

-- Freshness (5 separate queries)
SELECT MAX(window_start) AS last_row FROM btc_ohlc_1m;
SELECT MAX(window_start) AS last_row FROM sentiment_30m;
SELECT MAX(window_start) AS last_row FROM volatility_pred;
SELECT MAX(finished_at) AS last_row FROM pipeline_lineage;
SELECT MAX(changed_at) AS last_row FROM audit_log;
```

#### 9.2.2 Row 2 — Null % Heatmap + Data Freshness

| Left (½) | Right (½) |
|---|---|
| Heatmap: rows = tables, columns = column names, color = null_percent (green → yellow → red). Pivot `data_quality_stats` into matrix for `px.imshow`. | Freshness table: 5 tables with `last_data_at` timestamp + age (computed as `NOW() - last_data_at`), icons: ✅ <30min, 🟡 <2h, 🔴 >2h |

#### 9.2.3 Row 3 — Column Profiling Table

Full-width table of per-column stats: null_count, total_rows, null_percent, distinct_count, type_mismatch, min_value, max_value, mean_value, quality_status + CSV download.

#### 9.2.4 Row 4 — Validation History + Pass/Fail Donut

| Left (⅔) | Right (⅓) |
|---|---|
| Scatter timeline: `started_at` vs quality_status, color by OK (green) / FAILED (red) | Donut: count OK vs FAILED + summary text "x% passed" |

---

### 9.3 Sub-tab: Data Catalog

**Purpose:** Metadata exploration, lineage, and business glossary.

**Source:** `table_metadata`, `business_glossary`, `column_lineage`, `information_schema`.

#### 9.3.1 Row 1 — Sankey Lineage Diagram

Sankey diagram showing data flow from sources → tables → consumers. Orange nodes, translucent links. Edges built from hardcoded mappings + `column_lineage` table.

```sql
-- Table list
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name;

-- Column info per table
SELECT c.column_name, c.data_type, c.is_nullable,
       pg_catalog.col_description(
           (SELECT oid FROM pg_class WHERE relname = '{table}'),
           c.ordinal_position
       ) AS comment
FROM information_schema.columns c
WHERE c.table_schema = 'public' AND c.table_name = '{table}'
ORDER BY c.ordinal_position;
```

#### 9.3.2 Row 2 — Table Metadata + Column Info

| Left (⅓) | Right (⅔) |
|---|---|
| Table dropdown picker + metadata panel: description, owner, sensitivity, refresh_frequency, source_system, retention_days | Column info table: column_name, data_type, is_nullable, comment |

#### 9.3.3 Row 3 — Column Lineage

| Left (½) | Right (½) |
|---|---|
| Upstream columns: which sources feed INTO the selected table (`source_table, source_column, target_column, transformation`) | Downstream columns: which targets consume FROM the selected table (`target_table, target_column, source_column, transformation`) |

```sql
-- Column lineage
SELECT source_table, source_column, target_table, target_column,
       transformation, pipeline_name
FROM column_lineage;

-- Filter upstream
SELECT source_table, source_column, target_column, transformation
FROM column_lineage WHERE target_table = '{table}';

-- Filter downstream
SELECT target_table, target_column, source_column, transformation
FROM column_lineage WHERE source_table = '{table}';
```

#### 9.3.4 Row 4 — Business Glossary

Full-width table of all business terms: term, definition, technical_table, technical_column, category, owner + CSV download.

```sql
SELECT term, definition, technical_table, technical_column, category, owner
FROM business_glossary ORDER BY category, term;
```

---

### 9.4 Sub-tab: Logs

**Purpose:** Application log exploration and analysis.

**Source:** `app_logs`.

#### 9.4.1 Filter Controls

Two dropdowns: Service (from `SELECT DISTINCT service`) + Level (All / INFO / WARNING / ERROR / CRITICAL).

#### 9.4.2 Row 1 — 3 KPI Cards

| KPI | Value | Computation |
|---|---|---|
| Total Logs | `N` | `len(logs_df)` client-side after filter |
| Errors | `N` (red if >0) | `len(logs_df[level == 'ERROR'])` client-side |
| Warnings | `N` | `len(logs_df[level == 'WARNING'])` client-side |

```sql
-- Filtered logs
SELECT timestamp, service, level, message FROM app_logs
{WHERE service = 'X' AND level = 'Y'}
ORDER BY timestamp DESC LIMIT 200;

-- Log level distribution 24h
SELECT level, COUNT(*) AS count FROM app_logs
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY level ORDER BY count DESC;

-- Error timeseries 7d
SELECT date_trunc('hour', timestamp) AS hour, COUNT(*) AS errors
FROM app_logs
WHERE level IN ('ERROR','CRITICAL')
  AND timestamp > NOW() - INTERVAL '7 days'
GROUP BY 1 ORDER BY 1;

-- Pipeline error summary 24h
SELECT pipeline_name AS service, COUNT(*) AS runs,
       SUM(CASE WHEN quality_status = 'failed' THEN 1 ELSE 0 END) AS failures,
       MAX(finished_at) AS last_run
FROM pipeline_lineage
WHERE finished_at > NOW() - INTERVAL '24 hours'
GROUP BY pipeline_name ORDER BY failures DESC;

-- Alert history
SELECT timestamp, service, level, message FROM app_logs
WHERE level IN ('ERROR','CRITICAL')
ORDER BY timestamp DESC LIMIT 30;
```

#### 9.4.3 Row 2 — Log Table + Level Donut

| Left (¾) | Right (¼) |
|---|---|
| Log table: timestamp, service, level, message, scrollable + CSV download | Donut: log level distribution (INFO/WARNING/ERROR/CRITICAL, colors: green/yellow/red/dark-red) |

#### 9.4.4 Row 3 — Error Timeseries + Pipeline Error Summary

| Left (½) | Right (½) |
|---|---|
| Bar chart: errors per hour over 7 days, red | Table: pipeline name, runs, failures, last_run, ordered by failures DESC |

#### 9.4.5 Row 4 — Recent Alerts

Table: last 30 ERROR/CRITICAL log records + CSV download.

---

## 10. All Data Tables Reference

### 10.1 Core Data Tables (PostgreSQL)

| Table | Purpose | Key Columns |
|---|---|---|
| `btc_ohlc_1m` | 1-minute OHLC from Binance via Spark | `window_start, open, high, low, close, volume, trade_count, volatility` |
| `sentiment_30m` | Twitter sentiment 30-min aggregation (FinVADER) | `window_start, compound_score, positive_ratio, negative_ratio, neutral_ratio, weighted_compound, tweet_count, data_quality` |
| `volatility_pred` | Real-time XGBoost volatility predictions | `window_start, predicted_vol_5m, rolling_vol_5m, price_range_ratio, vol_ratio, compound_score, minutes_since_sentiment, model_version, inference_latency_ms` |
| `btc_predictions` | Predicted vs actual volatility comparison | `window_start, predicted_vol, actual_vol, model_version, model_mae` |
| `model_performance` | Aggregated model evaluation metrics | `window_start, window_end, model_version, rmse, mae, n_predictions, checked_at` |
| `pipeline_lineage` | Every pipeline run metadata | `run_id, pipeline_name, source, target_table, rows_processed, rows_rejected, quality_status, started_at, finished_at, params` |
| `audit_log` | Automated INSERT/UPDATE/DELETE audit | `table_name, operation, old_data, new_data, changed_by, changed_at` |
| `app_logs` | Application log entries | `timestamp, service, level, message, run_id, extra` |
| `data_quality_stats` | Per-column profiling stats | `table_name, column_name, null_count, null_percent, distinct_count, type_mismatch, min_value, max_value, mean_value, quality_status, checked_at` |
| `table_metadata` | Centralized table documentation | `table_name, description, owner, sensitivity, refresh_frequency, source_system, retention_days` |
| `business_glossary` | Business terms mapped to tables/columns | `term, definition, technical_table, technical_column, category, owner` |
| `column_lineage` | Source-to-target column lineage | `source_table, source_column, target_table, target_column, transformation, pipeline_name` |

### 10.2 View

| View | Purpose | Columns |
|---|---|---|
| `v_ml_features` | Training features: OHLC + sentiment join + forward-fill | `window_start, rolling_vol_5m, price_range_ratio, vol_ratio, compound_score, positive_ratio, tweet_count, minutes_since_sentiment, target_vol_5m` |

### 10.3 Telegraf System Metrics Tables (auto-created)

| Table | Purpose | Key Columns |
|---|---|---|
| `cpu` | Host CPU metrics | `time, usage_idle, usage_system, usage_user` |
| `mem` | Host memory metrics | `time, total, available, used, used_percent` |
| `disk` | Host disk metrics | `time, path, total, used, used_percent` |
| `docker_container_cpu` | Per-container CPU % | `time, container_name, usage_percent` |
| `docker_container_mem` | Per-container memory | `time, container_name, usage_percent, usage, limit` |
| `docker_container_status` | Per-container status | `time, container_name, status, oomkilled` |

### 10.4 Trino / Hive Tables

| Table | Purpose | Key Columns |
|---|---|---|
| `hive.twitter_raw.tweets` | Raw tweet data in MinIO (Parquet) | `created_at, id_str, full_text, screen_name, retweet_count, favorite_count, followers_count, lang, keyword, compound` |
| `postgresql.public.btc_ohlc_1m` | PostgreSQL OHLC via Trino federated | Same as btc_ohlc_1m |

---

## 11. Data Processing: Derived Computations

### Computed Server-side (SQL Aggregation)

| Output | Source | SQL |
|---|---|---|
| Volume per hour | `btc_ohlc_1m` | `date_trunc('hour', window_start)`, `SUM(volume)`, `GROUP BY` |
| Feature mean values | `volatility_pred` | `AVG()` via `UNION ALL` for 3 features |
| Log level distribution | `app_logs` | `GROUP BY level`, `COUNT(*)`, 24h filter |
| Error timeseries | `app_logs` | `date_trunc('hour')`, `WHERE level IN ('ERROR','CRITICAL')`, `GROUP BY` |
| Pipeline success rate | `pipeline_lineage` | `SUM(CASE WHEN quality_status='ok'...) / COUNT(*)` |
| Completeness % | `data_quality_stats` | `AVG(100 - null_percent)` |
| Data freshness | 5 separate tables | `MAX(window_start)` per table |

### Computed Client-side (Python / Plotly)

| Output | Source | Transformation |
|---|---|---|
| Price delta 24h | `btc_ohlc_1m` | `(close.iloc[-1] / close.iloc[0] - 1) * 100` |
| CPU usage % | `cpu` | `100 - usage_idle` |
| System: running/total | `docker_container_status` | Count filter by `status == 'running'` |
| Residual | `btc_predictions` | `actual_vol - predicted_vol` |
| Data age (freshness) | 5 tables | `datetime.now(UTC) - last_data_at` |
| KPI counts (logs) | `app_logs` filtered | `len(df)`, `len(df[level == 'ERROR'])` |
| Pearson correlation | Trino query result | `scipy.stats.pearsonr(close, avg_compound)` |
| OLS regression line | Trino query result | `numpy.polyfit(close, avg_compound, 1)` |
| Sentiment color coding | `compound_score` | green if >0.05, yellow if >-0.05, red otherwise |
| Completeness color | `completeness_pct` | green if >95%, yellow if >85%, red otherwise |

### Computed in View (`v_ml_features`)

| Output | Source | Computation |
|---|---|---|
| `rolling_vol_5m` | `btc_ohlc_1m` | `STDDEV(ret) OVER (ROWS 4 PRECEDING AND CURRENT ROW)` |
| `price_range_ratio` | `btc_ohlc_1m` | `(high - low) / NULLIF(close, 0)` |
| `vol_ratio` | `btc_ohlc_1m` | `volume / NULLIF(AVG(volume) OVER (ROWS 9 PRECEDING AND CURRENT ROW), 0)` |
| `target_vol_5m` | `btc_ohlc_1m` | `STDDEV(ret) OVER (ROWS 1 FOLLOWING AND 5 FOLLOWING)` |
| `compound_score` (ffill) | `sentiment_30m` | Forward-fill via `MAX(CASE WHEN) OVER (ROWS UNBOUNDED PRECEDING)` |
| `minutes_since_sentiment` | `sentiment_30m` | `EXTRACT(EPOCH FROM (window_start - last_sentiment_ts)) / 60.0` |

---

## 12. 7-Feature XGBoost Model (for context)

The model predicts 5-minute forward volatility. Features used for both training and inference:

| # | Feature | Type | Description |
|---|---|---|---|
| 1 | `rolling_vol_5m` | float64 | Rolling std dev of 5-minute close returns |
| 2 | `price_range_ratio` | float64 | `(high - low) / close` range ratio |
| 3 | `vol_ratio` | float64 | `volume / avg_volume_10min` ratio |
| 4 | `compound_score` | float64 | FinVADER compound sentiment |
| 5 | `positive_ratio` | float64 | Proportion of positive tweets |
| 6 | `tweet_count` | float64 | Number of tweets |
| 7 | `minutes_since_sentiment` | float64 | Staleness of sentiment data |

**Target:** `target_vol_5m` (std dev of 5-min forward returns)

**Hyperparameters:** XGBRegressor(n_estimators=300, max_depth=4, lr=0.05, subsample=0.8, colsample_bytree=0.8, min_child_weight=5, early_stopping_rounds=20)

**Training:** TimeSeriesSplit 5-fold CV, StandardScaler per fold. Auto-promote to `production` alias if MAE < 0.0015.

---

## 13. Implementation Status

| Page | Status |
|---|---|
| Summary | **Orphaned** — code exists but not in sidebar navigation |
| Market Overview | Active |
| Volatility Analytics | Active |
| Sentiment Analytics | Active |
| Pipeline Operations | **Orphaned** — code exists but not in sidebar navigation |
| Operations (System Health / Data Quality / Data Catalog / Logs) | Active |

### Known Issues

| # | Issue | Severity |
|---|---|---|
| 1 | 6 KPI cards in `st.columns(6)` too narrow on screens <1366px | High |
| 2 | Gauge charts stacked vertically (660px), unbalanced layout | Medium |
| 3 | Auto-refresh `sleep(30)` causes flicker on rerun | Medium |
| 4 | Chart heights inconsistent between paired columns | Medium |
| 5 | Logs KPI row uses `st.columns(4)` but only 3 values (25% wasted) | Low |
| 6 | Dead CSS class `.card` defined but unused | Low |
| 7 | No time-range selectors — all windows hardcoded | Feature gap |
| 8 | No dark/light theme toggle | Feature gap |
