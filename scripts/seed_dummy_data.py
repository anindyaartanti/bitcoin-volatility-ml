import os
import random
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5434"))
PG_DB = os.getenv("PG_DB", "btcdb")
PG_USER = os.getenv("PG_USER", "kelompok4_ipbd")
PG_PASSWORD = os.getenv("PG_PASSWORD", "k4ipbd_postgres_2026")

RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

N_MINUTES = 1440


def _conn():
    import psycopg2
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def generate_ohlc() -> pd.DataFrame:
    now = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
    base_price = 30000.0
    prices = [base_price]
    for _ in range(N_MINUTES - 1):
        ret = np.random.normal(0, 0.0008)
        prices.append(prices[-1] * (1 + ret))

    rows = []
    for i in range(N_MINUTES):
        ts = now - timedelta(minutes=N_MINUTES - 1 - i)
        cp = prices[i]
        op = cp * (1 + np.random.uniform(-0.002, 0.002))
        hi = max(op, cp) * (1 + abs(np.random.normal(0, 0.003)))
        lo = min(op, cp) * (1 - abs(np.random.normal(0, 0.003)))
        vol = np.random.uniform(0.5, 15)
        tc = int(np.random.uniform(100, 800))

        ret_price = (prices[i] - prices[i - 1]) / prices[i - 1] if i > 0 else 0
        lookback = max(0, i - 4)
        recent_rets = [
            (prices[j] - prices[j - 1]) / prices[j - 1]
            for j in range(lookback + 1, i + 1) if j > 0
        ]
        vol_val = float(np.std(recent_rets)) if len(recent_rets) > 1 else 0.0001

        rows.append({
            "window_start": ts, "window_end": ts + timedelta(minutes=1),
            "open": round(op, 2), "high": round(hi, 2),
            "low": round(lo, 2), "close": round(cp, 2),
            "volume": round(vol, 4), "trade_count": tc,
            "volatility": round(min(vol_val, 0.05), 8),
        })
    return pd.DataFrame(rows)


def generate_sentiment() -> pd.DataFrame:
    now = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
    rows = []
    for i in range(48):
        ts = now - timedelta(minutes=48 * 30 - i * 30)
        compound = np.random.normal(0.05, 0.3)
        compound = max(-1, min(1, compound))
        pos = max(0, np.random.normal(0.3, 0.15))
        neg = max(0, np.random.normal(0.2, 0.12))
        neu = max(0, 1 - pos - neg)
        total = pos + neg + neu
        pos /= total; neg /= total; neu /= total
        weighted = compound + np.random.normal(0, 0.02)
        weighted = max(-1, min(1, weighted))
        tweets = int(np.random.poisson(50))
        dq = "ok" if tweets > 5 else "low_sample"
        rows.append({
            "window_start": ts, "window_end": ts + timedelta(minutes=30),
            "compound_score": round(compound, 4),
            "positive_ratio": round(pos, 4),
            "negative_ratio": round(neg, 4),
            "neutral_ratio": round(neu, 4),
            "weighted_compound": round(weighted, 4),
            "tweet_count": tweets,
            "data_quality": dq, "source_file": f"tweets/dummy_{i}.parquet",
        })
    return pd.DataFrame(rows)


def generate_predictions(ohlc: pd.DataFrame, sent: pd.DataFrame) -> pd.DataFrame:
    models = ["v1", "v2", "v3"]
    now = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
    model_versions = (
        ["v1"] * (N_MINUTES // 3)
        + ["v2"] * (N_MINUTES // 3)
        + ["v3"] * (N_MINUTES - 2 * (N_MINUTES // 3))
    )

    ohlc_dict = ohlc.set_index("window_start").to_dict("index")
    sent_dict = sent.set_index("window_start").to_dict("index")

    rows = []
    for i in range(N_MINUTES):
        ts = now - timedelta(minutes=N_MINUTES - 1 - i)
        o = ohlc_dict.get(ts, {})

        minutes_ago = (i % 30) + 1
        sent_ts = ts - timedelta(minutes=minutes_ago)
        s = sent_dict.get(sent_ts, {})

        pred = (o.get("volatility", 0.001) * np.random.uniform(0.8, 1.2))
        pred = max(0.00005, pred)
        rolling_vol = o.get("volatility", 0.001) * np.random.uniform(0.9, 1.1)
        prr = ((o.get("high", 30001) - o.get("low", 29999)) / max(o.get("close", 30000), 0.01))
        vr = o.get("volume", 1) / max(np.random.uniform(0.5, 2), 0.01)
        latency = int(np.random.uniform(15, 80))

        rows.append({
            "window_start": ts,
            "predicted_vol_5m": round(pred, 8),
            "rolling_vol_5m": round(min(rolling_vol, 0.05), 8),
            "price_range_ratio": round(min(prr, 0.01), 6),
            "vol_ratio": round(vr, 4),
            "compound_score": round(s.get("compound_score", 0), 4),
            "minutes_since_sentiment": round(minutes_ago + np.random.uniform(0, 2), 2),
            "model_version": model_versions[i],
            "inference_latency_ms": latency,
        })
    return pd.DataFrame(rows)


def generate_lineage() -> pd.DataFrame:
    now = datetime.now(tz=timezone.utc)
    pipelines = [
        ("spark_streaming", "btc_ticker_raw", "btc_ohlc_1m", 1440, 5),
        ("sentiment_pipeline", "twitter_api", "sentiment_30m", 48, 2),
        ("model_training", "v_ml_features", "mlflow_registry", 1000, 0),
    ]
    rows = []
    for pname, src, tgt, rp, rr in pipelines:
        for run in range(5):
            ts = now - timedelta(hours=run * 4 + random.randint(0, 30))
            status = "ok" if random.random() > 0.15 else "failed"
            rows.append({
                "pipeline_name": pname, "source": src, "target_table": tgt,
                "rows_processed": rp - random.randint(0, 50),
                "rows_rejected": rr + random.randint(0, 3),
                "quality_status": status,
                "started_at": ts, "finished_at": ts + timedelta(seconds=random.randint(30, 300)),
                "params": '{"batch_id": %d}' % run,
            })
    return pd.DataFrame(rows)


def generate_audit() -> pd.DataFrame:
    now = datetime.now(tz=timezone.utc)
    tables = ["btc_ohlc_1m", "sentiment_30m", "volatility_pred"]
    ops = ["INSERT", "UPDATE", "DELETE"]
    rows = []
    for i in range(10):
        ts = now - timedelta(hours=i * 2 + random.randint(0, 30))
        rows.append({
            "table_name": random.choice(tables),
            "operation": random.choice(ops),
            "changed_by": random.choice(["spark_streaming", "sentiment_pipeline", "airflow"]),
            "changed_at": ts,
        })
    return pd.DataFrame(rows)


def insert_df(conn, df: pd.DataFrame, table: str):
    with conn.cursor() as cur:
        cols = df.columns.tolist()
        placeholders = ", ".join(["%s"] * len(cols))
        col_names = ", ".join(cols)
        conflict = {
            "btc_ohlc_1m": "ON CONFLICT (window_start) DO NOTHING",
            "sentiment_30m": "ON CONFLICT (window_start) DO NOTHING",
            "volatility_pred": "ON CONFLICT (window_start) DO NOTHING",
            "pipeline_lineage": "",
            "audit_log": "",
        }.get(table, "")

        sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) {conflict}"

        for _, row in df.iterrows():
            vals = []
            for c in cols:
                v = row[c]
                if isinstance(v, (datetime, pd.Timestamp)):
                    vals.append(v.to_pydatetime() if hasattr(v, "to_pydatetime") else v)
                elif pd.isna(v):
                    vals.append(None)
                else:
                    vals.append(v)
            cur.execute(sql, vals)
    conn.commit()


def main():
    print("Generating dummy data...")
    ohlc = generate_ohlc()
    sent = generate_sentiment()
    pred = generate_predictions(ohlc, sent)
    lineage = generate_lineage()
    audit = generate_audit()

    print(f"  btc_ohlc_1m:     {len(ohlc)} rows")
    print(f"  sentiment_30m:   {len(sent)} rows")
    print(f"  volatility_pred: {len(pred)} rows")
    print(f"  pipeline_lineage: {len(lineage)} rows")
    print(f"  audit_log:       {len(audit)} rows")

    print("Connecting to PostgreSQL...")
    conn = _conn()
    try:
        insert_df(conn, ohlc, "btc_ohlc_1m")
        print("  btc_ohlc_1m inserted.")
        insert_df(conn, sent, "sentiment_30m")
        print("  sentiment_30m inserted.")
        insert_df(conn, pred, "volatility_pred")
        print("  volatility_pred inserted.")
        insert_df(conn, lineage, "pipeline_lineage")
        print("  pipeline_lineage inserted.")
        insert_df(conn, audit, "audit_log")
        print("  audit_log inserted.")
    finally:
        conn.close()

    print("Done. Semua tabel terisi data dummy.")


if __name__ == "__main__":
    main()
