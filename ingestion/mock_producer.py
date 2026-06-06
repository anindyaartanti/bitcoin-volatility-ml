"""
ingestion/mock_producer.py
===========================
Mock producer pengganti Binance WebSocket untuk testing lokal
ketika tidak ada akses internet.

Generate data kline BTC/USDT palsu setiap 60 detik dengan format
identik dengan binance_websocket.py, sehingga stream_processor.py
tidak perlu diubah sama sekali.
"""

import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("mock_producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "btc_ticker_raw")
INTERVAL_SECONDS        = int(os.getenv("MOCK_INTERVAL", "60"))  # 60 = 1 menit


def create_producer(retries=5, delay=5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info("Kafka producer terhubung ke %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except KafkaError as e:
            logger.warning("Percobaan %d/%d gagal: %s", attempt, retries, e)
            if attempt < retries:
                time.sleep(delay)
    logger.error("Tidak bisa konek ke Kafka. Keluar.")
    sys.exit(1)


# Harga awal BTC — simulasi random walk
_price = 67000.0


def generate_kline(closed: bool = True) -> dict:
    """Generate satu pesan kline dengan random walk sederhana."""
    global _price

    # Random walk: ±0.3% per candle
    change_pct = random.uniform(-0.003, 0.003)
    open_price  = _price
    close_price = round(open_price * (1 + change_pct), 2)
    high_price  = round(max(open_price, close_price) * random.uniform(1.0, 1.002), 2)
    low_price   = round(min(open_price, close_price) * random.uniform(0.998, 1.0), 2)
    volume      = round(random.uniform(5.0, 50.0), 4)
    trade_count = random.randint(50, 500)

    _price = close_price  # update harga untuk candle berikutnya

    now = datetime.now(tz=timezone.utc)
    kline_start = now.replace(second=0, microsecond=0)
    kline_end   = kline_start.replace(second=59)

    return {
        "event_type":  "kline",
        "event_time":  now.isoformat(),
        "symbol":      "BTCUSDT",
        "interval":    "1m",
        "kline_start": kline_start.isoformat(),
        "kline_end":   kline_end.isoformat(),
        "open":        open_price,
        "high":        high_price,
        "low":         low_price,
        "close":       close_price,
        "volume":      volume,
        "trade_count": trade_count,
        "is_closed":   closed,
    }


producer = create_producer()


def shutdown(signum, frame):
    logger.info("Shutdown diterima. Menutup producer...")
    producer.flush()
    producer.close()
    sys.exit(0)

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)


def main():
    logger.info(
        "Mock producer dimulai. Kirim kline setiap %d detik ke topic '%s'",
        INTERVAL_SECONDS, KAFKA_TOPIC,
    )

    candle_count = 0
    while True:
        # Kirim beberapa tick is_closed=False dulu (opsional, mirip perilaku asli)
        for _ in range(3):
            tick = generate_kline(closed=False)
            producer.send(KAFKA_TOPIC, value=tick)
            time.sleep(1)

        # Kirim kline closed — ini yang diproses Spark
        closed_kline = generate_kline(closed=True)
        future = producer.send(KAFKA_TOPIC, value=closed_kline)
        producer.flush()

        candle_count += 1
        logger.info(
            "Candle #%d dikirim: close=%.2f high=%.2f low=%.2f vol=%.4f",
            candle_count,
            closed_kline["close"],
            closed_kline["high"],
            closed_kline["low"],
            closed_kline["volume"],
        )

        # Tunggu sisa interval
        time.sleep(max(1, INTERVAL_SECONDS - 3))


if __name__ == "__main__":
    main()