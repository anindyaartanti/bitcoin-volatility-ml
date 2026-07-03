#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
export MINIO_ENDPOINT=http://localhost:9002
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=k4ipbd_minio_2026
export PREFECT_API_URL=http://localhost:4201/api

case "${1:-help}" in
  sentiment)
    python prefect/flows/sentiment_flow.py
    ;;
  train)
    python prefect/flows/training_flow.py
    ;;
  deploy)
    cd prefect/flows && python deploy_all.py
    ;;
  *)
    echo "Usage: $0 {sentiment|train|deploy}"
    exit 1
    ;;
esac
