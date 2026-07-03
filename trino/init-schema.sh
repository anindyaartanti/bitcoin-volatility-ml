#!/bin/bash






set -euo pipefail

TRINO_CONTAINER="trino"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Registering MinIO Parquet schema in Hive Metastore ==="
docker exec -i "$TRINO_CONTAINER" trino < "$SCRIPT_DIR/ddl/register_twitter_raw.sql"

echo "=== Verifying ==="
docker exec -i "$TRINO_CONTAINER" trino --execute "SHOW TABLES FROM hive.twitter_raw;"
docker exec -i "$TRINO_CONTAINER" trino --execute "SELECT COUNT(*) AS total_parquet_files FROM hive.twitter_raw.tweets;"

echo "=== Done ==="
