"""
ingestion/binance_websocket.py
==============================
Stream harga BTC/USDT real-time dari Binance WebSocket
dan kirim ke Kafka topic 'btc_ticker_raw'.

Jalankan:
    python binance_websocket.py

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS  (default: localhost:9092)
    KAFKA_TOPIC              (default: btc_ticker_raw)
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer
from kafka.errors import KafkaError
import ssl as ssl_lib

# ─── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("binance_producer")

# ─── Konfigurasi ────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "btc_ticker_raw")
BINANCE_WS_URL          = "wss://stream.binance.com:443/stream?streams=btcusdt@trade/btcusdt@kline_1m"

# ─── Kafka Producer ─────────────────────────────────────────
def create_producer(retries: int = 5, delay: int = 5) -> KafkaProducer:
    """Buat KafkaProducer dengan retry saat koneksi gagal."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",           # tunggu semua replica ack
                retries=3,
                max_block_ms=10_000,
            )
            logger.info("Kafka producer terhubung ke %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except KafkaError as e:
            logger.warning("Koneksi Kafka gagal (percobaan %d/%d): %s", attempt, retries, e)
            if attempt < retries:
                time.sleep(delay)
    logger.error("Tidak bisa terhubung ke Kafka setelah %d percobaan. Keluar.", retries)
    sys.exit(1)


producer = create_producer()


# ─── Transformasi Pesan Binance ──────────────────────────────
def parse_trade(data: dict) -> dict:
    """Parse pesan @trade dari Binance."""
    return {
        "event_type":  "trade",
        "event_time":  datetime.fromtimestamp(data["T"] / 1000, tz=timezone.utc).isoformat(),
        "symbol":      data["s"],
        "trade_id":    data["t"],
        "price":       float(data["p"]),
        "quantity":    float(data["q"]),
        "is_buyer_mm": data["m"],          # True = market sell
    }


def parse_kline(data: dict) -> dict:
    """Parse pesan @kline_1m dari Binance."""
    k = data["k"]
    return {
        "event_type":   "kline",
        "event_time":   datetime.fromtimestamp(data["E"] / 1000, tz=timezone.utc).isoformat(),
        "symbol":       k["s"],
        "interval":     k["i"],
        "kline_start":  datetime.fromtimestamp(k["t"] / 1000, tz=timezone.utc).isoformat(),
        "kline_end":    datetime.fromtimestamp(k["T"] / 1000, tz=timezone.utc).isoformat(),
        "open":         float(k["o"]),
        "high":         float(k["h"]),
        "low":          float(k["l"]),
        "close":        float(k["c"]),
        "volume":       float(k["v"]),
        "trade_count":  int(k["n"]),
        "is_closed":    k["x"],            # True jika candle sudah tutup
    }


# ─── WebSocket Handlers ──────────────────────────────────────
def on_message(ws, message: str):
    try:
        outer = json.loads(message)
        stream = outer.get("stream", "")
        data   = outer.get("data", {})

        if "trade" in stream:
            payload = parse_trade(data)
        elif "kline" in stream:
            payload = parse_kline(data)
        else:
            logger.debug("Stream tidak dikenal: %s", stream)
            return

        future = producer.send(KAFKA_TOPIC, value=payload)
        future.add_errback(lambda e: logger.error("Gagal kirim ke Kafka: %s", e))

        logger.debug("Terkirim ke Kafka [%s]: price=%s", payload["event_type"], payload.get("price") or payload.get("close"))

    except Exception as e:
        logger.exception("Error saat memproses pesan WebSocket: %s", e)


def on_error(ws, error):
    logger.error("WebSocket error: %s", error)


def on_close(ws, close_status_code, close_msg):
    logger.warning("WebSocket ditutup (code=%s, msg=%s)", close_status_code, close_msg)


def on_open(ws):
    logger.info("WebSocket Binance terhubung. Mulai streaming BTC/USDT...")


# ─── Graceful Shutdown ───────────────────────────────────────
def shutdown(signum, frame):
    logger.info("Sinyal shutdown diterima. Menutup producer...")
    producer.flush()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


# ─── Main ────────────────────────────────────────────────────
def main():
    reconnect_delay = 5   # detik
    while True:
        logger.info("Menghubungkan ke Binance WebSocket: %s", BINANCE_WS_URL)
        ws = websocket.WebSocketApp(
            BINANCE_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(
            ping_interval=30,
            ping_timeout=10,
            sslopt={"cert_reqs": ssl_lib.CERT_NONE}
        )
        logger.warning("Koneksi terputus. Reconnect dalam %ds...", reconnect_delay)
        time.sleep(reconnect_delay)


if __name__ == "__main__":
    main()
