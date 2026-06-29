"""
security/crypto.py
===================
Enkripsi data in-transit untuk Kafka message payloads.

Menggunakan Fernet (symmetric encryption):
  - Algorithm: AES-128-CBC + HMAC-SHA256
  - Key: 32-byte URL-safe base64-encoded key
  - Output: base64-encoded ciphertext (ASCII-safe)

Cara generate key:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

Usage (Producer — encrypt sebelum kirim ke Kafka):
    >>> from cryptography.fernet import Fernet
    >>> import json, os
    >>> cipher = Fernet(os.environ["ENCRYPTION_KEY"].encode())
    >>> payload = {"symbol": "BTCUSDT", "price": "50000.00"}
    >>> encrypted = cipher.encrypt(json.dumps(payload).encode("utf-8"))
    >>> kafka_producer.send("topic", value=encrypted)

Usage (Consumer — decrypt setelah baca dari Kafka):
    >>> decrypted = cipher.decrypt(encrypted_value).decode("utf-8")
    >>> data = json.loads(decrypted)
"""

import json
import os

from cryptography.fernet import Fernet


def get_cipher() -> Fernet | None:
    key = os.getenv("ENCRYPTION_KEY", "")
    if not key:
        return None
    return Fernet(key.encode())


def encrypt_dict(cipher: Fernet | None, data: dict) -> bytes:
    if cipher is None:
        return json.dumps(data).encode("utf-8")
    return cipher.encrypt(json.dumps(data).encode("utf-8"))


def decrypt_to_str(cipher: Fernet | None, raw: bytes) -> str:
    if cipher is None:
        return raw.decode("utf-8", errors="replace")
    try:
        return cipher.decrypt(raw).decode("utf-8")
    except Exception:
        return raw.decode("utf-8", errors="replace")
