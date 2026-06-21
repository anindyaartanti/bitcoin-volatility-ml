FROM ghcr.io/mlflow/mlflow:v2.10.2

RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir psycopg2-binary boto3
