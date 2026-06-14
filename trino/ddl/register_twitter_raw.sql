-- =============================================================
-- trino/ddl/register_twitter_raw.sql
-- Daftarkan schema + external table untuk Parquet tweets di MinIO
-- Jalankan: docker exec -i trino trino < register_twitter_raw.sql
-- =============================================================

CREATE SCHEMA IF NOT EXISTS hive.twitter_raw
  WITH (location = 's3a://twitter-raw/');

DROP TABLE IF EXISTS hive.twitter_raw.tweets;

CREATE TABLE hive.twitter_raw.tweets (
  created_at         TIMESTAMP,
  id_str             VARCHAR,
  full_text          VARCHAR,
  text               VARCHAR,
  screen_name        VARCHAR,
  retweet_count      BIGINT,
  favorite_count     BIGINT,
  followers_count    BIGINT,
  lang               VARCHAR,
  keyword            VARCHAR,
  compound           DOUBLE
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://twitter-raw/tweets/'
);
