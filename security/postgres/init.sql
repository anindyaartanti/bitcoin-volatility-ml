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

-- ─── btc_predictions ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS btc_predictions (
    id              BIGSERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ NOT NULL UNIQUE,
    predicted_vol   NUMERIC(10,8) NOT NULL CHECK (predicted_vol >= 0),
    actual_vol      NUMERIC(10,8) CHECK (actual_vol >= 0),
    model_version   VARCHAR(20),
    model_mae       NUMERIC(10,8),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_predictions_window_start ON btc_predictions (window_start DESC);

-- ─── v_ml_features view ─────────────────────────────────────
CREATE OR REPLACE VIEW v_ml_features AS
WITH
bounds AS (
    SELECT MIN(window_start) AS t_min, MAX(window_start) AS t_max
    FROM btc_ohlc_1m
),
returns_calc AS (
    SELECT
        window_start,
        high, low, close, volume,
        (close - LAG(close) OVER (ORDER BY window_start))
            / NULLIF(LAG(close) OVER (ORDER BY window_start), 0) AS ret
    FROM btc_ohlc_1m
),
ohlc_calc AS (
    SELECT
        window_start, close,
        STDDEV(ret) OVER (
            ORDER BY window_start ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS rolling_vol_5m,
        (high - low) / NULLIF(close, 0) AS price_range_ratio,
        volume / NULLIF(
            AVG(volume) OVER (ORDER BY window_start ROWS BETWEEN 9 PRECEDING AND CURRENT ROW), 0
        ) AS vol_ratio,
        STDDEV(ret) OVER (
            ORDER BY window_start ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING
        ) AS target_vol_5m
    FROM returns_calc
),
sentiment_series AS (
    SELECT
        gs.t AS window_start,
        s.compound_score, s.positive_ratio, s.tweet_count,
        s.window_start AS sentiment_window_start
    FROM bounds,
         generate_series(bounds.t_min, bounds.t_max, INTERVAL '1 minute') AS gs(t)
    LEFT JOIN sentiment_30m s
           ON s.window_start <= gs.t AND s.window_end > gs.t
),
sentiment_ffill AS (
    SELECT
        window_start,
        MAX(CASE WHEN compound_score IS NOT NULL THEN compound_score END)
            OVER (ORDER BY window_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS compound_score,
        MAX(CASE WHEN positive_ratio IS NOT NULL THEN positive_ratio END)
            OVER (ORDER BY window_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS positive_ratio,
        MAX(CASE WHEN tweet_count IS NOT NULL THEN tweet_count END)
            OVER (ORDER BY window_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS tweet_count,
        MAX(CASE WHEN sentiment_window_start IS NOT NULL THEN sentiment_window_start END)
            OVER (ORDER BY window_start ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS last_sentiment_ts
    FROM sentiment_series
)
SELECT
    o.window_start,
    o.rolling_vol_5m, o.price_range_ratio, o.vol_ratio,
    sf.compound_score, sf.positive_ratio,
    sf.tweet_count::FLOAT AS tweet_count,
    EXTRACT(EPOCH FROM (o.window_start - sf.last_sentiment_ts)) / 60.0
        AS minutes_since_sentiment,
    o.target_vol_5m
FROM ohlc_calc o
JOIN sentiment_ffill sf ON sf.window_start = o.window_start
WHERE o.target_vol_5m IS NOT NULL
  AND o.rolling_vol_5m IS NOT NULL
  AND sf.compound_score IS NOT NULL
ORDER BY o.window_start;

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
    FOREACH tbl IN ARRAY ARRAY['btc_ohlc_1m','sentiment_30m','pipeline_lineage','btc_predictions'] LOOP
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
GRANT SELECT ON btc_ohlc_1m, sentiment_30m, pipeline_lineage, audit_log, btc_predictions TO dashboard_reader;
