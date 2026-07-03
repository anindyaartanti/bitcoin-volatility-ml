import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import psycopg2
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("fetch_historical")

HOURS_BACK = int(os.getenv("HOURS_BACK", "48"))
BINANCE_DOMAIN = "api.binance.com"
DOH_URL = "https://dns.google/resolve"
DOH_UA = "Mozilla/5.0"


BINANCE_PATH = "/api/v3/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "1m"
LIMIT = 1000

PG_HOST = os.getenv("PGBOUNCER_HOST", "localhost")
PG_PORT = int(os.getenv("PGBOUNCER_PORT", "6432"))
PG_DB   = os.getenv("APP_DB_NAME", "btcdb")
PG_USER = os.getenv("APP_DB_USER", "btcadmin")
PG_PASS = os.getenv("APP_DB_PASSWORD", "gantiPasswordAman123")


def resolve_via_doh(domain: str) -> str | None:
    seen: set[str] = set()
    cursor = domain
    for _ in range(5):
        if cursor in seen:
            break
        seen.add(cursor)
        try:
            resp = requests.get(
                DOH_URL,
                params={"name": cursor, "type": "A"},
                headers={"User-Agent": DOH_UA},
                timeout=10,
            )
            data = resp.json()
            for answer in data.get("Answer", []):
                t = answer.get("type")
                if t == 1:
                    return answer["data"]
                if t == 5:
                    cursor = answer["data"].rstrip(".")
                    break
            else:
                log.warning("DOH: no A/CNAME for %s", cursor)
                break
        except Exception as e:
            log.warning("DOH gagal: %s", e)
            return None
    log.warning("DOH: could not resolve %s", domain)
    return None


def fetch_klines(start_ms: int, end_ms: int, resolved_ip: str) -> list:
    headers = {"Host": BINANCE_DOMAIN}
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "limit": LIMIT,
        "startTime": start_ms,
        "endTime": end_ms,
    }
    url = f"https://{resolved_ip}{BINANCE_PATH}"
    resp = requests.get(url, params=params, headers=headers, timeout=30, verify=False)
    resp.raise_for_status()
    data = resp.json()
    if not data:
        log.info("  API balikin array kosong — mungkin nggak ada data di range ini")
    return data


def calc_volatility(open_p: float, high: float, low: float, close: float) -> float:
    prices = [open_p, high, low, close]
    arr = np.array(prices)
    returns = np.diff(arr) / arr[:-1]
    if len(returns) < 2:
        return 0.0
    return float(np.std(returns, ddof=1))


def main():
    parser = argparse.ArgumentParser(description="Fetch historical BTC/USDT 1m OHLC from Binance")
    parser.add_argument("--days", type=int, default=None, help="Days to fetch (overrides HOURS_BACK)")
    parser.add_argument("--hours", type=int, default=None, help="Hours to fetch (overrides HOURS_BACK)")
    args = parser.parse_args()

    global HOURS_BACK
    if args.days is not None:
        HOURS_BACK = args.days * 24
    if args.hours is not None:
        HOURS_BACK = args.hours

    log.info("Resolve %s via DoH...", BINANCE_DOMAIN)
    resolved_ip = resolve_via_doh(BINANCE_DOMAIN)
    if not resolved_ip:
        log.error("Gagal resolve IP via DoH.")
        sys.exit(1)
    log.info("Resolved IP: %s", resolved_ip)

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=HOURS_BACK)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)
    log.info("Fetch BTC/USDT 1m dari %s ke %s (%d jam)", start, now, HOURS_BACK)

    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()

    total_ins = 0
    total_fetched = 0
    cursor_ms = start_ms
    while cursor_ms < end_ms:
        raw = fetch_klines(cursor_ms, end_ms, resolved_ip)
        if not raw:
            break

        for k in raw:
            ts_ms = k[0]
            ws = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            we = ws + timedelta(minutes=1)
            o = float(k[1]); h = float(k[2]); l = float(k[3]); c = float(k[4])
            v = float(k[5]); tc = int(k[8])
            vol = calc_volatility(o, h, l, c)

            try:
                cur.execute(
                    """
                    INSERT INTO btc_ohlc_1m
                        (window_start, window_end, open, high, low, close,
                         volume, trade_count, volatility)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (window_start) DO NOTHING
                    """,
                    (ws, we, o, h, l, c, v, tc, vol),
                )
                if cur.rowcount > 0:
                    total_ins += 1
            except Exception as e:
                log.warning("Skip row %s: %s", ws, e)

        conn.commit()
        total_fetched += len(raw)
        log.info("  Fetched %d klines (total %d), inserted %d", len(raw), total_fetched, total_ins)

        last_ts = raw[-1][0]
        cursor_ms = last_ts + 1
        if len(raw) < LIMIT:
            break

    cur.close(); conn.close()
    log.info("Selesai. %d baris baru di btc_ohlc_1m.", total_ins)


if __name__ == "__main__":
    main()
