-- ============================================================
-- security/postgres/init.sql
-- Inisialisasi skema, role, dan tabel untuk database aplikasi
-- Dieksekusi otomatis saat container PostgreSQL pertama kali start
-- ============================================================

-- Pastikan berada di database aplikasi (btcdb)
\connect btcdb

-- ─────────────────────────────────────────────────────────────
-- EXTENSION
-- ─────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ─────────────────────────────────────────────────────────────
-- ROLE / USER (Least Privilege)
-- ─────────────────────────────────────────────────────────────

-- User untuk Streamlit dashboard (hanya baca)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dashboard_reader') THEN
    CREATE ROLE dashboard_reader WITH LOGIN PASSWORD 'reader_pass_ganti';
  END IF;
END$$;

-- User untuk pipeline ML (baca + tulis tabel tertentu)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ml_writer') THEN
    CREATE ROLE ml_writer WITH LOGIN PASSWORD 'writer_pass_ganti';
  END IF;
END$$;

-- ─────────────────────────────────────────────────────────────
-- TABEL: btc_ohlc_1m
-- Diisi oleh Spark Structured Streaming (Fase 2)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS btc_ohlc_1m (
    id             BIGSERIAL PRIMARY KEY,
    window_start   TIMESTAMPTZ NOT NULL,
    window_end     TIMESTAMPTZ NOT NULL,
    open           NUMERIC(18, 8) NOT NULL,
    high           NUMERIC(18, 8) NOT NULL,
    low            NUMERIC(18, 8) NOT NULL,
    close          NUMERIC(18, 8) NOT NULL,
    volume              NUMERIC(24, 8) NOT NULL,
    trade_count         INTEGER DEFAULT 0,
    rolling_volatility  NUMERIC(12, 8),
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ohlc_window_start ON btc_ohlc_1m (window_start DESC);

-- ─────────────────────────────────────────────────────────────
-- TABEL: sentiment_raw
-- Diisi oleh batch_sentiment.py (FinVADER) per tweet
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sentiment_raw (
    id              BIGSERIAL PRIMARY KEY,
    tweet_id        VARCHAR(64) NOT NULL UNIQUE,
    tweet_text      TEXT,
    tweet_created_at TIMESTAMPTZ,
    scrape_time     TIMESTAMPTZ,
    sentiment_score NUMERIC(6, 4),
    positive        NUMERIC(6, 4),
    negative        NUMERIC(6, 4),
    neutral         NUMERIC(6, 4),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sentiment_raw_scrape
    ON sentiment_raw (scrape_time DESC);

-- ─────────────────────────────────────────────────────────────
-- TABEL: sentiment_hourly
-- Diisi oleh Spark Batch dari data Twitter (Fase 3)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sentiment_hourly (
    id              BIGSERIAL PRIMARY KEY,
    hour_timestamp  TIMESTAMPTZ NOT NULL UNIQUE,
    avg_sentiment   NUMERIC(6, 4),
    std_sentiment   NUMERIC(6, 4),
    mention_count   INTEGER DEFAULT 0,
    positive_count  INTEGER DEFAULT 0,
    negative_count  INTEGER DEFAULT 0,
    neutral_count   INTEGER DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sentiment_hour ON sentiment_hourly (hour_timestamp DESC);

-- ─────────────────────────────────────────────────────────────
-- TABEL: predictions
-- Diisi oleh Spark Streaming + ML inference (Fase 4)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS predictions (
    id                    BIGSERIAL PRIMARY KEY,
    timestamp             TIMESTAMPTZ NOT NULL,
    predicted_volatility  NUMERIC(12, 8),
    actual_volatility     NUMERIC(12, 8),   -- diisi setelah window 5 menit berlalu
    sentiment_score       NUMERIC(6, 4),
    close_price           NUMERIC(18, 8),
    features_json         JSONB,
    model_version         VARCHAR(64),
    created_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pred_timestamp ON predictions (timestamp DESC);

-- ─────────────────────────────────────────────────────────────
-- TABEL: metadata_table
-- Lineage dan statistik per pipeline run
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS metadata_table (
    id             BIGSERIAL PRIMARY KEY,
    dataset_name   VARCHAR(128) NOT NULL,
    source         VARCHAR(256),
    location       VARCHAR(512),        -- path MinIO atau nama tabel
    record_count   INTEGER,
    run_timestamp  TIMESTAMPTZ DEFAULT NOW(),
    pipeline_name  VARCHAR(128),
    status         VARCHAR(32) DEFAULT 'success'
);

-- ─────────────────────────────────────────────────────────────
-- TABEL: audit_log
-- Audit trail setiap aksi penting di pipeline
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit_log (
    id           BIGSERIAL PRIMARY KEY,
    event_time   TIMESTAMPTZ DEFAULT NOW(),
    username     VARCHAR(64),
    action       VARCHAR(64),           -- INSERT, UPDATE, DELETE, SELECT, PIPELINE_RUN, dll.
    table_name   VARCHAR(128),
    record_id    VARCHAR(128),
    details      TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_log (event_time DESC);

-- ─────────────────────────────────────────────────────────────
-- GRANT PRIVILEGES
-- ─────────────────────────────────────────────────────────────
GRANT SELECT ON btc_ohlc_1m, sentiment_hourly, predictions, metadata_table TO dashboard_reader;
GRANT SELECT, INSERT, UPDATE ON btc_ohlc_1m, sentiment_hourly, predictions, metadata_table, audit_log TO ml_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ml_writer;
