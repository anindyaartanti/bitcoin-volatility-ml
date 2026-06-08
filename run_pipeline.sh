#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
export MINIO_ENDPOINT=http://localhost:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin123

case "${1:-help}" in
  twitter)
    python ingestion/twitter_batch.py \
      --query "bitcoin OR BTC -is:retweet lang:en" \
      --limit 200 \
      --minio-endpoint localhost:9000
    ;;
  sentiment)
    python processing/batch_sentiment.py \
      --hours 6 \
      --minio-endpoint localhost:9000
    ;;
  train)
    python prefect/flows/model_training.py
    ;;
  all)
    $0 twitter
    $0 sentiment
    ;;
  *)
    echo "Usage: $0 {twitter|sentiment|train|all}"
    exit 1
    ;;
esac
