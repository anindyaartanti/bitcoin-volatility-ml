"""
ingestion/binance_producer.py
==============================
Binance @trade WebSocket → Kafka btc_ticker_raw
- Exponential backoff, max 5 retries, lalu Telegram alert
- Pesan malformed → btc_ticker_dlq
"""

import json
import logging
import os
import signal
import sys
import time

import requests
import websocket
from cryptography.fernet import Fernet
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("binance_producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "btc_ticker_raw")
KAFKA_DLQ_TOPIC         = os.getenv("KAFKA_DLQ_TOPIC", "btc_ticker_dlq")
TELEGRAM_BOT_TOKEN      = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID        = os.getenv("TELEGRAM_CHAT_ID", "")
BINANCE_HOST            = "stream.binance.com"
BINANCE_WS_PATH         = "/ws/btcusdt@trade"
MAX_RETRIES             = 5

# ─── Encryption (Kafka in-transit) ─────────────────────────────
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "")
_cipher = Fernet(ENCRYPTION_KEY.encode()) if ENCRYPTION_KEY else None

# DNS-over-HTTPS resolver untuk bypass DNS hijacking (ISP Indonesia)
DOH_URL = "https://dns.google/resolve"


def resolve_via_doh(domain: str) -> str | None:
    try:
        resp = requests.get(
            DOH_URL,
            params={"name": domain, "type": "A"},
            timeout=10,
        )
        data = resp.json()
        for answer in data.get("Answer", []):
            if answer.get("type") == 1:
                return answer["data"]
        logger.error("DoH: no A record for %s", domain)
    except Exception as e:
        logger.error("DoH resolution failed: %s", e)
    return None

REQUIRED_FIELDS = {"T", "p", "q", "s", "t"}


def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message},
            timeout=10,
        )
    except Exception as e:
        logger.warning("Telegram alert gagal: %s", e)


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v if isinstance(v, (bytes, bytearray)) else json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        max_block_ms=10_000,
    )


producer: KafkaProducer = None


def on_message(ws, message: str):
    try:
        data = json.loads(message)
        if not REQUIRED_FIELDS.issubset(data.keys()):
            raise ValueError(f"Missing fields: {REQUIRED_FIELDS - data.keys()}")
        payload = {
            "event_time": data["T"],   # ms epoch, Spark akan parse
            "symbol":     data["s"],
            "trade_id":   data["t"],
            "price":      data["p"],   # string, Spark cast ke double
            "quantity":   data["q"],
            "is_buyer_mm": data.get("m", False),
        }
        if _cipher:
            encrypted = _cipher.encrypt(json.dumps(payload).encode("utf-8"))
            producer.send(KAFKA_TOPIC, value=encrypted)
        else:
            producer.send(KAFKA_TOPIC, value=payload)
    except (json.JSONDecodeError, ValueError, KeyError) as e:
        logger.warning("Malformed message → DLQ: %s", e)
        try:
            producer.send(KAFKA_DLQ_TOPIC, value={"raw": message, "error": str(e)})
        except Exception:
            pass
    except Exception as e:
        logger.error("Unexpected error: %s", e)


def on_error(ws, error):
    logger.error("WebSocket error: %s", error)


def on_close(ws, code, msg):
    logger.warning("WebSocket closed (code=%s)", code)


_connected_successfully = False


def on_open(ws):
    global _connected_successfully
    _connected_successfully = True
    logger.info("WebSocket connected → streaming btcusdt@trade")


def shutdown(signum, frame):
    logger.info("Shutdown signal received")
    if producer:
        producer.flush()
        producer.close()
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def main():
    global producer
    producer = create_producer()

    # Resolve IP asli via DoH (bypass DNS hijacking)
    resolved_ip = resolve_via_doh(BINANCE_HOST)
    if not resolved_ip:
        logger.error("Gagal resolve %s via DoH — fallback ke DNS sistem (rentan hijack)", BINANCE_HOST)
        resolved_ip = BINANCE_HOST

    if resolved_ip != BINANCE_HOST:
        ws_url = f"wss://{resolved_ip}:9443{BINANCE_WS_PATH}"
    else:
        ws_url = f"wss://{BINANCE_HOST}:9443{BINANCE_WS_PATH}"

    attempt = 0
    while True:
        if attempt >= MAX_RETRIES:
            msg = f"[binance_producer] Gagal reconnect setelah {MAX_RETRIES} percobaan. WebSocket dihentikan."
            logger.error(msg)
            send_telegram(msg)
            sys.exit(1)

        delay = min(2 ** attempt, 60)
        if attempt > 0:
            logger.warning("Reconnect percobaan %d/%d dalam %ds...", attempt, MAX_RETRIES, delay)
            time.sleep(delay)

        global _connected_successfully
        _connected_successfully = False
        attempt += 1
        logger.info("Menghubungkan ke %s (percobaan %d)", ws_url, attempt)
        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(
            ping_interval=30,
            ping_timeout=10,
            sslopt={"server_hostname": BINANCE_HOST},
            host=BINANCE_HOST,
        )
        # Jika berhasil connect sebelumnya, reset counter
        if _connected_successfully:
            attempt = 0


if __name__ == "__main__":
    main()
