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
    CREATE ROLE dashboard_reader WITH LOGIN PASSWORD 'k4ipbd_reader_2026';
  END IF;
END$$;

-- ─── DROP OLD SCHEMA (migration from dev) ────────────────────
DROP TABLE IF EXISTS sentiment_hourly CASCADE;
DROP TABLE IF EXISTS predictions CASCADE;
DROP TABLE IF EXISTS metadata_table CASCADE;
-- NOTE: Ganti password dashboard_reader via:
--   ALTER ROLE dashboard_reader PASSWORD '<isi k4ipbd_reader_2026 dari .env>'
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

-- ─── volatility_pred ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS volatility_pred (
    id                      BIGSERIAL PRIMARY KEY,
    window_start            TIMESTAMPTZ NOT NULL UNIQUE,
    predicted_vol_5m        NUMERIC(10,8) NOT NULL CHECK (predicted_vol_5m >= 0),
    rolling_vol_5m          NUMERIC(10,8),
    price_range_ratio       NUMERIC(8,6),
    vol_ratio               NUMERIC(8,4),
    compound_score          NUMERIC(6,4),
    minutes_since_sentiment NUMERIC(6,2),
    model_version           VARCHAR(20),
    inference_latency_ms    INTEGER,
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vp_window ON volatility_pred (window_start DESC);

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
-- ─── system_metrics (Telegraf auto-creates: cpu, mem, disk, docker_container_cpu, docker_container_mem) ──
-- Tables created at runtime by Telegraf on first metric flush (~15s after telegraf starts)
-- Column names follow Telegraf convention: field names become columns, tag names become columns

-- ─── data_quality_stats ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_quality_stats (
    id             BIGSERIAL PRIMARY KEY,
    table_name     VARCHAR(50) NOT NULL,
    column_name    VARCHAR(50) NOT NULL,
    null_count     INTEGER,
    total_rows     INTEGER,
    null_percent   NUMERIC(6,2),
    distinct_count INTEGER,
    type_mismatch  INTEGER DEFAULT 0,
    min_value      DOUBLE PRECISION,
    max_value      DOUBLE PRECISION,
    mean_value     DOUBLE PRECISION,
    quality_status VARCHAR(10) DEFAULT 'ok',
    checked_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_checked_at ON data_quality_stats (checked_at DESC);

-- ─── table_metadata ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS table_metadata (
    id                BIGSERIAL PRIMARY KEY,
    table_name        VARCHAR(50) NOT NULL UNIQUE,
    description       TEXT,
    owner             VARCHAR(50),
    data_steward      VARCHAR(50),
    sensitivity       VARCHAR(20) CHECK (sensitivity IN ('public','internal','confidential','restricted')),
    refresh_frequency VARCHAR(30),
    source_system     TEXT,
    retention_days    INTEGER,
    pii_columns       TEXT,
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO table_metadata (table_name, description, owner, data_steward, sensitivity, refresh_frequency, source_system, retention_days, pii_columns) VALUES
('btc_ohlc_1m',       'OHLC agregasi 1 menit dari Binance WebSocket via Spark Streaming', 'kelompok4_ipbd', 'fatih', 'internal',    'real-time (1m)', 'Binance WebSocket → Kafka → Spark', NULL, NULL),
('sentiment_30m',     'Skor sentimen Twitter aggregasi 30 menit (FinVADER)',            'kelompok4_ipbd', 'fatih', 'internal',    'batch (30m)',    'Twitter/X scrape → Prefect → MinIO → FinVADER', NULL, NULL),
('volatility_pred',   'Prediksi volatilitas 5 menit dari XGBoost inference real-time',   'kelompok4_ipbd', 'fatih', 'internal',    'real-time (30s)', 'Spark Streaming + MLflow Registry', NULL, NULL),
('btc_predictions',   'Prediksi vs aktual volatilitas (MLflow model registry)',          'kelompok4_ipbd', 'fatih', 'internal',    'batch (daily)',  'Prefect training flow + MLflow', NULL, NULL),
('pipeline_lineage',  'Data lineage: setiap run pipeline source/target/row/status',       'kelompok4_ipbd', 'fatih', 'internal',    'per-run',        'Semua pipeline', NULL, NULL),
('audit_log',         'Audit trail otomatis via trigger INSERT/UPDATE/DELETE',            'kelompok4_ipbd', 'fatih', 'internal',    'per-event',      'PostgreSQL trigger', NULL, NULL),
('data_quality_stats','Statistik profiling data: null %, min, max, mean per kolom',       'kelompok4_ipbd', 'fatih', 'internal',    'hourly',         'Prefect data-quality-check flow', NULL, NULL)
ON CONFLICT (table_name) DO NOTHING;

-- ─── business_glossary ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS business_glossary (
    id               BIGSERIAL PRIMARY KEY,
    term             VARCHAR(100) NOT NULL UNIQUE,
    definition       TEXT NOT NULL,
    technical_table  VARCHAR(50),
    technical_column VARCHAR(50),
    category         VARCHAR(50),
    owner            VARCHAR(50),
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO business_glossary (term, definition, technical_table, technical_column, category, owner) VALUES
('OHLC',                       'Open-High-Low-Close: harga pembuka, tertinggi, terendah, penutup dalam 1 menit', 'btc_ohlc_1m', NULL, 'Market Data', 'kelompok4_ipbd'),
('Volatilitas',                'Standar deviasi log-return harga dalam window 1 menit',                        'btc_ohlc_1m', 'volatility', 'Risk Metric', 'kelompok4_ipbd'),
('Volume Perdagangan',         'Total kuantitas BTC yang diperdagangkan dalam 1 menit',                       'btc_ohlc_1m', 'volume', 'Market Data', 'kelompok4_ipbd'),
('Compound Score',             'Skor sentimen FinVADER: -1 (negatif) sampai +1 (positif)',                    'sentiment_30m', 'compound_score', 'Sentiment', 'kelompok4_ipbd'),
('Positive Ratio',             'Proporsi tweet dengan compound > 0.05 dalam window 30 menit',                 'sentiment_30m', 'positive_ratio', 'Sentiment', 'kelompok4_ipbd'),
('Weighted Compound',          'Skor compound dikalikan jumlah like, dinormalisasi',                          'sentiment_30m', 'weighted_compound', 'Sentiment', 'kelompok4_ipbd'),
('Predicted Vol 5m',           'Prediksi volatilitas 5 menit ke depan dari XGBoost model',                    'volatility_pred', 'predicted_vol_5m', 'ML Prediction', 'kelompok4_ipbd'),
('Rolling Vol 5m',             'Std dev return 5 menit rolling (fitur input model)',                          'volatility_pred', 'rolling_vol_5m', 'Risk Metric', 'kelompok4_ipbd'),
('Inference Latency',          'Waktu yang dibutuhkan model untuk menghasilkan satu prediksi (ms)',            'volatility_pred', 'inference_latency_ms', 'ML Metric', 'kelompok4_ipbd'),
('Data Quality Flag',          'Indikator kualitas data: ok (>5 tweet), low_sample (<5), stale (forward-fill)', 'sentiment_30m', 'data_quality', 'Data Governance', 'kelompok4_ipbd'),
('Pipeline Lineage',           'Metadata run setiap pipeline: source, target, row count, status',             'pipeline_lineage', NULL, 'Data Governance', 'kelompok4_ipbd'),
('Target Vol 5m',              'Std dev return 5 menit ke depan — target supervised learning',                'v_ml_features', 'target_vol_5m', 'ML Target', 'kelompok4_ipbd'),
('Minutes Since Sentiment',    'Selisih menit sejak data sentimen terakhir diperbarui (staleness)',           'volatility_pred', 'minutes_since_sentiment', 'Data Governance', 'kelompok4_ipbd'),
('Forward-fill',               'Mengisi window gagal dengan data window sukses terakhir (stale flag)',        NULL, NULL, 'Data Governance', 'kelompok4_ipbd'),
('Circuit Breaker',            'Mekanisme safety: hentikan write jika gagal 5x berturut, reset setelah 60s',  NULL, NULL, 'System', 'kelompok4_ipbd')
ON CONFLICT (term) DO NOTHING;

-- ─── COMMENT ON ──────────────────────────────────────────────

-- btc_ohlc_1m
COMMENT ON TABLE btc_ohlc_1m IS 'OHLC agregasi per 1 menit dari Binance WebSocket (btcusdt@trade) via Spark Structured Streaming';
COMMENT ON COLUMN btc_ohlc_1m.window_start IS 'Waktu mulai window 1 menit (UTC)';
COMMENT ON COLUMN btc_ohlc_1m.window_end IS 'Waktu akhir window 1 menit (UTC)';
COMMENT ON COLUMN btc_ohlc_1m.open IS 'Harga trade pertama dalam window';
COMMENT ON COLUMN btc_ohlc_1m.high IS 'Harga trade tertinggi dalam window';
COMMENT ON COLUMN btc_ohlc_1m.low IS 'Harga trade terendah dalam window';
COMMENT ON COLUMN btc_ohlc_1m.close IS 'Harga trade terakhir dalam window';
COMMENT ON COLUMN btc_ohlc_1m.volume IS 'Total kuantitas BTC dalam window';
COMMENT ON COLUMN btc_ohlc_1m.trade_count IS 'Jumlah trade dalam window';
COMMENT ON COLUMN btc_ohlc_1m.volatility IS 'Std dev log-return harga per trade dalam window';

-- sentiment_30m
COMMENT ON TABLE sentiment_30m IS 'Skor sentimen Twitter/X aggregasi per 30 menit dari FinVADER scoring';
COMMENT ON COLUMN sentiment_30m.compound_score IS 'FinVADER compound score, range [-1 (negatif), +1 (positif)]';
COMMENT ON COLUMN sentiment_30m.positive_ratio IS 'Proporsi tweet dengan compound > 0.05 dalam window';
COMMENT ON COLUMN sentiment_30m.negative_ratio IS 'Proporsi tweet dengan compound < -0.05 dalam window';
COMMENT ON COLUMN sentiment_30m.neutral_ratio IS 'Proporsi tweet dengan compound dalam [-0.05, 0.05]';
COMMENT ON COLUMN sentiment_30m.weighted_compound IS 'Compound score dikalikan like count, dinormalisasi';
COMMENT ON COLUMN sentiment_30m.tweet_count IS 'Jumlah tweet dalam window 30 menit';
COMMENT ON COLUMN sentiment_30m.data_quality IS 'Kualitas data: ok (>5 tweet), low_sample (<5), stale (forward-fill)';

-- volatility_pred
COMMENT ON TABLE volatility_pred IS 'Hasil prediksi volatilitas 5 menit ke depan dari XGBoost inference real-time';
COMMENT ON COLUMN volatility_pred.predicted_vol_5m IS 'Prediksi std dev return 5 menit ke depan';
COMMENT ON COLUMN volatility_pred.rolling_vol_5m IS 'Std dev return 5 menit rolling (fitur OHLC)';
COMMENT ON COLUMN volatility_pred.price_range_ratio IS '(high - low) / close — rasio range harga';
COMMENT ON COLUMN volatility_pred.vol_ratio IS 'volume / avg_volume_10min — rasio volume';
COMMENT ON COLUMN volatility_pred.compound_score IS 'Skor sentimen FinVADER saat inference (dari cache)';
COMMENT ON COLUMN volatility_pred.minutes_since_sentiment IS 'Menit sejak sentimen terakhir diperbarui (staleness)';
COMMENT ON COLUMN volatility_pred.model_version IS 'Versi model XGBoost dari MLflow Registry';
COMMENT ON COLUMN volatility_pred.inference_latency_ms IS 'Latency inference dalam milidetik';

-- btc_predictions
COMMENT ON TABLE btc_predictions IS 'Perbandingan prediksi vs aktual volatilitas untuk evaluasi model';
COMMENT ON COLUMN btc_predictions.predicted_vol IS 'Nilai prediksi volatilitas';
COMMENT ON COLUMN btc_predictions.actual_vol IS 'Nilai aktual volatilitas dari data OHLC';
COMMENT ON COLUMN btc_predictions.model_mae IS 'MAE model saat training';

-- pipeline_lineage
COMMENT ON TABLE pipeline_lineage IS 'Data lineage: mencatat setiap run pipeline dengan source, target, row count, kualitas';
COMMENT ON COLUMN pipeline_lineage.run_id IS 'UUID unik per pipeline run';
COMMENT ON COLUMN pipeline_lineage.pipeline_name IS 'Nama pipeline: sentiment_pipeline, model_training, spark_streaming, system_health_check, data_quality_check';
COMMENT ON COLUMN pipeline_lineage.source IS 'Sumber data yang diproses';
COMMENT ON COLUMN pipeline_lineage.target_table IS 'Tabel tujuan penulisan';
COMMENT ON COLUMN pipeline_lineage.rows_processed IS 'Jumlah baris yang berhasil diproses';
COMMENT ON COLUMN pipeline_lineage.rows_rejected IS 'Jumlah baris yang ditolak';
COMMENT ON COLUMN pipeline_lineage.quality_status IS 'Status kualitas: ok, failed, promoted, stale';
COMMENT ON COLUMN pipeline_lineage.params IS 'Metadata tambahan dalam format JSONB';

-- audit_log
COMMENT ON TABLE audit_log IS 'Audit trail otomatis via trigger: mencatat semua INSERT/UPDATE/DELETE di tabel utama';
COMMENT ON COLUMN audit_log.table_name IS 'Nama tabel yang diubah';
COMMENT ON COLUMN audit_log.operation IS 'Operasi: INSERT, UPDATE, atau DELETE';
COMMENT ON COLUMN audit_log.old_data IS 'Data sebelum perubahan (JSONB), null untuk INSERT';
COMMENT ON COLUMN audit_log.new_data IS 'Data setelah perubahan (JSONB), null untuk DELETE';
COMMENT ON COLUMN audit_log.changed_by IS 'User PostgreSQL yang melakukan perubahan';

-- v_ml_features
COMMENT ON VIEW v_ml_features IS 'Fitur untuk training XGBoost: JOIN OHLC 1m + sentimen 30m (forward-fill) + target vol 5m';

-- data_quality_stats
COMMENT ON TABLE data_quality_stats IS 'Statistik profiling data quality per tabel per kolom';
COMMENT ON COLUMN data_quality_stats.null_count IS 'Jumlah null values dalam kolom';
COMMENT ON COLUMN data_quality_stats.null_percent IS 'Persentase null values';
COMMENT ON COLUMN data_quality_stats.distinct_count IS 'Jumlah nilai unik dalam kolom';
COMMENT ON COLUMN data_quality_stats.type_mismatch IS 'Jumlah nilai yang gagal konversi tipe data';

-- table_metadata
COMMENT ON TABLE table_metadata IS 'Metadata terpusat: deskripsi, owner, sensitivity, frekuensi refresh tiap tabel';
COMMENT ON COLUMN table_metadata.sensitivity IS 'Klasifikasi sensitivitas: public, internal, confidential, restricted';

-- business_glossary
COMMENT ON TABLE business_glossary IS 'Business glossary: istilah bisnis yang dipetakan ke tabel/kolom teknis';

-- ─── column_lineage ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS column_lineage (
    id                 BIGSERIAL PRIMARY KEY,
    source_table       VARCHAR(50) NOT NULL,
    source_column      VARCHAR(50) NOT NULL,
    target_table       VARCHAR(50) NOT NULL,
    target_column      VARCHAR(50) NOT NULL,
    transformation     TEXT,
    pipeline_name      VARCHAR(50),
    created_at         TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO column_lineage (source_table, source_column, target_table, target_column, transformation, pipeline_name) VALUES
-- Spark Streaming: Kafka trades → OHLC
('btc_ticker_raw', 'price',     'btc_ohlc_1m', 'open',      'FIRST in 1m tumbling window', 'spark_streaming'),
('btc_ticker_raw', 'price',     'btc_ohlc_1m', 'high',      'MAX in 1m tumbling window', 'spark_streaming'),
('btc_ticker_raw', 'price',     'btc_ohlc_1m', 'low',       'MIN in 1m tumbling window', 'spark_streaming'),
('btc_ticker_raw', 'price',     'btc_ohlc_1m', 'close',     'LAST in 1m tumbling window', 'spark_streaming'),
('btc_ticker_raw', 'quantity',  'btc_ohlc_1m', 'volume',    'SUM in 1m tumbling window', 'spark_streaming'),
('btc_ticker_raw', 'price',     'btc_ohlc_1m', 'trade_count','COUNT in 1m tumbling window', 'spark_streaming'),
('btc_ticker_raw', 'price',     'btc_ohlc_1m', 'volatility','stddev(log-returns) in 1m window', 'spark_streaming'),

-- Spark Streaming: OHLC features → volatility_pred
('btc_ohlc_1m', 'close',    'volatility_pred', 'rolling_vol_5m',       'rolling stddev(close.pct_change(), 5)', 'spark_streaming'),
('btc_ohlc_1m', 'high',     'volatility_pred', 'price_range_ratio',    '(high - low) / close', 'spark_streaming'),
('btc_ohlc_1m', 'low',      'volatility_pred', 'price_range_ratio',    '(high - low) / close', 'spark_streaming'),
('btc_ohlc_1m', 'volume',   'volatility_pred', 'vol_ratio',            'volume / avg_volume_10min', 'spark_streaming'),
('sentiment_30m', 'compound_score',  'volatility_pred', 'compound_score',          'sentiment cache (refresh 30m)', 'spark_streaming'),
('sentiment_30m', 'positive_ratio',  'volatility_pred', 'positive_ratio',          'sentiment cache (refresh 30m)', 'spark_streaming'),
('sentiment_30m', 'tweet_count',     'volatility_pred', 'tweet_count',             'sentiment cache (refresh 30m)', 'spark_streaming'),
('sentiment_30m', 'window_start',    'volatility_pred', 'minutes_since_sentiment', 'NOW - window_start (minutes)', 'spark_streaming'),

-- XGBoost inference: all features → prediction
('volatility_pred', 'rolling_vol_5m',          'volatility_pred', 'predicted_vol_5m', 'XGBoost Regressor (7 features → volatility)', 'spark_streaming'),
('volatility_pred', 'price_range_ratio',       'volatility_pred', 'predicted_vol_5m', 'XGBoost Regressor (7 features → volatility)', 'spark_streaming'),
('volatility_pred', 'vol_ratio',               'volatility_pred', 'predicted_vol_5m', 'XGBoost Regressor (7 features → volatility)', 'spark_streaming'),
('volatility_pred', 'compound_score',          'volatility_pred', 'predicted_vol_5m', 'XGBoost Regressor (7 features → volatility)', 'spark_streaming'),
('volatility_pred', 'positive_ratio',          'volatility_pred', 'predicted_vol_5m', 'XGBoost Regressor (7 features → volatility)', 'spark_streaming'),
('volatility_pred', 'tweet_count',             'volatility_pred', 'predicted_vol_5m', 'XGBoost Regressor (7 features → volatility)', 'spark_streaming'),
('volatility_pred', 'minutes_since_sentiment', 'volatility_pred', 'predicted_vol_5m', 'XGBoost Regressor (7 features → volatility)', 'spark_streaming'),

-- Sentiment pipeline: tweets → sentiment_30m
('tweets', 'full_text', 'sentiment_30m', 'compound_score',    'FinVADER compound sentiment [-1, 1]', 'sentiment_pipeline'),
('tweets', 'full_text', 'sentiment_30m', 'positive_ratio',    'COUNT(compound > 0.05) / total', 'sentiment_pipeline'),
('tweets', 'full_text', 'sentiment_30m', 'negative_ratio',    'COUNT(compound < -0.05) / total', 'sentiment_pipeline'),
('tweets', 'full_text', 'sentiment_30m', 'neutral_ratio',     '1 - positive_ratio - negative_ratio', 'sentiment_pipeline'),
('tweets', 'full_text', 'sentiment_30m', 'weighted_compound', 'compound × like_count / SUM(like_count)', 'sentiment_pipeline'),
('tweets', 'id_str',    'sentiment_30m', 'tweet_count',       'COUNT DISTINCT after dedup', 'sentiment_pipeline'),

-- Training flow: v_ml_features → MLflow model
('v_ml_features', 'rolling_vol_5m',          'mlflow', 'btc_volatility_xgb', 'XGBoost training feature 1/7', 'model_training'),
('v_ml_features', 'price_range_ratio',       'mlflow', 'btc_volatility_xgb', 'XGBoost training feature 2/7', 'model_training'),
('v_ml_features', 'vol_ratio',               'mlflow', 'btc_volatility_xgb', 'XGBoost training feature 3/7', 'model_training'),
('v_ml_features', 'compound_score',          'mlflow', 'btc_volatility_xgb', 'XGBoost training feature 4/7', 'model_training'),
('v_ml_features', 'positive_ratio',          'mlflow', 'btc_volatility_xgb', 'XGBoost training feature 5/7', 'model_training'),
('v_ml_features', 'tweet_count',             'mlflow', 'btc_volatility_xgb', 'XGBoost training feature 6/7', 'model_training'),
('v_ml_features', 'minutes_since_sentiment', 'mlflow', 'btc_volatility_xgb', 'XGBoost training feature 7/7', 'model_training'),

-- DQ profiling: source columns → data_quality_stats
('btc_ohlc_1m',     '*', 'data_quality_stats', 'null_count',     'COUNT rows WHERE column IS NULL', 'data_quality_check'),
('btc_ohlc_1m',     '*', 'data_quality_stats', 'distinct_count', 'COUNT DISTINCT column values', 'data_quality_check'),
('btc_ohlc_1m',     '*', 'data_quality_stats', 'min_value',      'MIN of numeric column', 'data_quality_check'),
('btc_ohlc_1m',     '*', 'data_quality_stats', 'max_value',      'MAX of numeric column', 'data_quality_check'),
('btc_ohlc_1m',     '*', 'data_quality_stats', 'mean_value',     'AVG of numeric column', 'data_quality_check'),
('sentiment_30m',   '*', 'data_quality_stats', 'null_count',     'COUNT rows WHERE column IS NULL', 'data_quality_check'),
('sentiment_30m',   '*', 'data_quality_stats', 'distinct_count', 'COUNT DISTINCT column values', 'data_quality_check'),
('sentiment_30m',   '*', 'data_quality_stats', 'min_value',      'MIN of numeric column', 'data_quality_check'),
('sentiment_30m',   '*', 'data_quality_stats', 'max_value',      'MAX of numeric column', 'data_quality_check'),
('sentiment_30m',   '*', 'data_quality_stats', 'mean_value',     'AVG of numeric column', 'data_quality_check'),
('volatility_pred', '*', 'data_quality_stats', 'null_count',     'COUNT rows WHERE column IS NULL', 'data_quality_check'),
('volatility_pred', '*', 'data_quality_stats', 'distinct_count', 'COUNT DISTINCT column values', 'data_quality_check'),
('volatility_pred', '*', 'data_quality_stats', 'min_value',      'MIN of numeric column', 'data_quality_check'),
('volatility_pred', '*', 'data_quality_stats', 'max_value',      'MAX of numeric column', 'data_quality_check'),
('volatility_pred', '*', 'data_quality_stats', 'mean_value',     'AVG of numeric column', 'data_quality_check')
ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_cl_target ON column_lineage (target_table, target_column);

-- ─── model_performance ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS model_performance (
    id              BIGSERIAL PRIMARY KEY,
    model_version   VARCHAR(20) NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    rmse            NUMERIC(10,8),
    mae             NUMERIC(10,8),
    n_predictions   INTEGER,
    checked_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mp_checked ON model_performance (checked_at DESC);

-- ─── app_logs ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS app_logs (
    id          BIGSERIAL PRIMARY KEY,
    timestamp   TIMESTAMPTZ DEFAULT NOW(),
    service     VARCHAR(50) NOT NULL,
    level       VARCHAR(10) NOT NULL CHECK (level IN ('DEBUG','INFO','WARNING','ERROR','CRITICAL')),
    message     TEXT NOT NULL,
    run_id      UUID,
    extra       JSONB
);
CREATE INDEX IF NOT EXISTS idx_logs_ts ON app_logs (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_svc ON app_logs (service, level);

GRANT SELECT ON btc_ohlc_1m, sentiment_30m, volatility_pred, btc_predictions, pipeline_lineage, audit_log, data_quality_stats, table_metadata, business_glossary, column_lineage, model_performance, app_logs TO dashboard_reader;

-- ─── Grant kelompok4_ipbd (full write access untuk pipeline & Trino) ────
GRANT USAGE ON SCHEMA public TO kelompok4_ipbd;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO kelompok4_ipbd;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO kelompok4_ipbd;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO kelompok4_ipbd;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO kelompok4_ipbd;

-- ─── Grant marquez db owner untuk Flyway DDL ─────────────────
-- (dijalankan di create_multiple_db.sh, diulangi di sini untuk safety)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'marquez') THEN
    CREATE ROLE marquez WITH LOGIN PASSWORD 'k4ipbd_marquez_2026';
  END IF;
END$$;
GRANT ALL PRIVILEGES ON DATABASE marquezdb TO marquez;
