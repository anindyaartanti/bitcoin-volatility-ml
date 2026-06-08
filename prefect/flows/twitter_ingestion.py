import os
import sys
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from prefect import flow, task

load_dotenv()

sys.path.insert(0, "/opt/ingestion")


@task(retries=2, retry_delay_seconds=60)
def scrape_tweets() -> int:
    from twitter_batch import run_ingestion

    queries = [
        "bitcoin OR BTC -is:retweet lang:en",
        "#bitcoin OR #BTC -is:retweet lang:en",
        "bitcoin price prediction -is:retweet lang:en",
    ]
    limit = int(os.getenv("TWITTER_SCRAPE_LIMIT", "200"))
    total = run_ingestion(queries=queries, limit=limit)
    return total


@task
def log_audit(tweet_count: int):
    import psycopg2

    try:
        conn = psycopg2.connect(
            host=os.getenv("APP_DB_HOST", "postgres"),
            port=int(os.getenv("APP_DB_PORT", "5432")),
            dbname=os.getenv("APP_DB_NAME", "btcdb"),
            user=os.getenv("APP_DB_USER", "btcadmin"),
            password=os.getenv("APP_DB_PASSWORD", ""),
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit_log (username, action, table_name, details)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "prefect",
                    "PIPELINE_RUN",
                    "twitter_raw (MinIO)",
                    f"twitter_ingestion flow selesai. Total tweet: {tweet_count}.",
                ),
            )
            conn.commit()
        conn.close()
    except Exception as e:
        print(f"Warning: gagal catat audit log: {e}")


@task
def trigger_sentiment_processing():
    api_url = os.getenv("PREFECT_API_URL", "http://prefect-server:4200/api")
    try:
        resp = requests.get(f"{api_url}/flows/", params={"name": "sentiment-processing"})
        resp.raise_for_status()
        flows = resp.json()
        if not flows:
            print("Warning: Flow 'sentiment-processing' belum terdaftar, skip trigger")
            return
        flow_id = flows[0]["id"]
        run_resp = requests.post(
            f"{api_url}/flow_runs/",
            json={"flow_id": flow_id, "state": {"type": "SCHEDULED"}},
        )
        if run_resp.ok:
            print(f"sentiment-processing flow run created: {run_resp.json().get('id')}")
        else:
            print(f"Warning: gagal trigger sentiment: {run_resp.text}")
    except Exception as e:
        print(f"Warning: gagal trigger sentiment_processing: {e}")


@flow(log_prints=True)
def twitter_ingestion_flow():
    tweet_count = scrape_tweets()
    if tweet_count > 0:
        log_audit(tweet_count)
        trigger_sentiment_processing()
    else:
        print("Tidak ada tweet yang di-scrape, skip audit dan trigger.")


if __name__ == "__main__":
    twitter_ingestion_flow()
