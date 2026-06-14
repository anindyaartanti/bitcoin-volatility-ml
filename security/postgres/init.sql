-- ============================================================
-- security/postgres/init.sql
-- Schema lengkap sesuai spesifikasi pipeline Bitcoin Volatility ML
-- ============================================================

\connect btcdb

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ─── ROLE ────────────────────────────────────────────────────
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dashboard_reader') THEN
    CREATE ROLE dashboard_reader WITH LOGIN PASSWORD 'reader_pass_ganti';
  END IF;
END$$;
-- NOTE: Ganti password dashboard_reader via:
--   ALTER ROLE dashboard_reader PASSWORD '<isi DASHBOARD_READER_PASSWORD dari .env>'
-- Password tidak bisa dibaca dari env saat init.sql dijalankan oleh postgres entrypoint.

-- ─── btc_ohlc_1m ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS btc_ohlc_1m (
    id           BIGSERIAL PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    open         NUMERIC(18,8) NOT NULL CHECK (open > 0),
    high         NUMERIC(18,8) NOT NULL CHECK (high >= open),
    low          NUMERIC(18,8) NOT NULL CHECK (low <= open AND low > 0),
    close        NUMERIC(18,8) NOT NULL CHECK (close > 0),
    volume       NUMERIC(24,8) NOT NULL CHECK (volume >= 0),
    trade_count  INTEGER NOT NULL CHECK (trade_count > 0),
    volatility   NUMERIC(10,8) CHECK (volatility >= 0),
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (window_start),
    CHECK (high >= low)
);

CREATE INDEX IF NOT EXISTS idx_ohlc_window_start ON btc_ohlc_1m (window_start DESC);

-- ─── sentiment_30m ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sentiment_30m (
    id               BIGSERIAL PRIMARY KEY,
    window_start     TIMESTAMPTZ NOT NULL UNIQUE,
    window_end       TIMESTAMPTZ NOT NULL,
    compound_score   NUMERIC(6,4) CHECK (compound_score BETWEEN -1 AND 1),
    positive_ratio   NUMERIC(5,4) CHECK (positive_ratio BETWEEN 0 AND 1),
    negative_ratio   NUMERIC(5,4) CHECK (negative_ratio BETWEEN 0 AND 1),
    neutral_ratio    NUMERIC(5,4) CHECK (neutral_ratio BETWEEN 0 AND 1),
    weighted_compound NUMERIC(6,4),
    tweet_count      INTEGER NOT NULL CHECK (tweet_count >= 0),
    data_quality     VARCHAR(12) DEFAULT 'ok' CHECK (data_quality IN ('ok', 'low_sample', 'stale')),
    source_file      TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sentiment_30m_window ON sentiment_30m (window_start DESC);

-- ─── pipeline_lineage ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_lineage (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_name   VARCHAR(50) NOT NULL,
    source          TEXT,
    target_table    VARCHAR(50),
    rows_processed  INTEGER,
    rows_rejected   INTEGER DEFAULT 0,
    quality_status  VARCHAR(10),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    params          JSONB
);

-- ─── audit_log ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit_log (
    id         BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(50),
    operation  VARCHAR(10) CHECK (operation IN ('INSERT','UPDATE','DELETE')),
    old_data   JSONB,
    new_data   JSONB,
    changed_by VARCHAR(50),
    changed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_changed_at ON audit_log (changed_at DESC);

-- ─── AUDIT TRIGGER FUNCTION ──────────────────────────────────
CREATE OR REPLACE FUNCTION fn_audit_trigger()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO audit_log (table_name, operation, old_data, new_data, changed_by)
    VALUES (
        TG_TABLE_NAME,
        TG_OP,
        CASE WHEN TG_OP = 'DELETE' THEN row_to_json(OLD)::jsonb ELSE NULL END,
        CASE WHEN TG_OP IN ('INSERT','UPDATE') THEN row_to_json(NEW)::jsonb ELSE NULL END,
        current_user
    );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Attach trigger ke 3 tabel
DO $$
DECLARE
    tbl TEXT;
BEGIN
    FOREACH tbl IN ARRAY ARRAY['btc_ohlc_1m','sentiment_30m','pipeline_lineage'] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = 'trg_audit_' || tbl
              AND tgrelid = tbl::regclass
        ) THEN
            EXECUTE format(
                'CREATE TRIGGER trg_audit_%I
                 AFTER INSERT OR UPDATE OR DELETE ON %I
                 FOR EACH ROW EXECUTE FUNCTION fn_audit_trigger()',
                tbl, tbl
            );
        END IF;
    END LOOP;
END$$;

-- ─── GRANTS ──────────────────────────────────────────────────
GRANT SELECT ON btc_ohlc_1m, sentiment_30m, pipeline_lineage, audit_log TO dashboard_reader;
