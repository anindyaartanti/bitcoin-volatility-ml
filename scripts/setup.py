"""
scripts/setup.py
================
Setup awal proyek - dipanggil oleh `make setup`.
Kompatibel Windows, Linux, Mac karena murni Python.
"""

import os
import secrets
import shutil
import sys
from pathlib import Path

# ── Warna terminal (opsional, fallback kalau tidak support) ──
try:
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    CYAN   = "\033[96m"
    BOLD   = "\033[1m"
    RESET  = "\033[0m"
    # Test apakah terminal support ANSI
    if sys.platform == "win32":
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
except Exception:
    GREEN = YELLOW = CYAN = BOLD = RESET = ""


def header(text):
    print(f"\n{BOLD}{CYAN}{'='*50}{RESET}")
    print(f"{BOLD}{CYAN}  {text}{RESET}")
    print(f"{BOLD}{CYAN}{'='*50}{RESET}")


def ok(text):
    print(f"  {GREEN}[OK]{RESET}   {text}")


def skip(text):
    print(f"  {YELLOW}[SKIP]{RESET} {text}")


def info(text):
    print(f"  {CYAN}[INFO]{RESET} {text}")


def divider():
    print(f"  {'-'*46}")


# ── Root proyek = direktori tempat Makefile berada ───────────
ROOT = Path(__file__).resolve().parent.parent


def step1_copy_env():
    """Salin .env.example → .env jika belum ada."""
    env_file     = ROOT / ".env"
    env_example  = ROOT / ".env.example"

    if not env_example.exists():
        print(f"  [ERROR] File .env.example tidak ditemukan di {ROOT}")
        sys.exit(1)

    if not env_file.exists():
        shutil.copy(env_example, env_file)
        ok(".env dibuat dari .env.example")
        print()
        print(f"  {YELLOW}WAJIB: Buka file .env dan isi nilai berikut:{RESET}")
        print(f"    - TWITTER_AUTH_TOKEN  (ambil dari cookie browser Twitter)")
        print(f"    - POSTGRES_PASSWORD   (ganti dari default)")
        print(f"    - MINIO_SECRET_KEY    (ganti dari default)")
        print(f"    - AIRFLOW_FERNET_KEY  (lihat output di bawah)")
        print(f"    - AIRFLOW_SECRET_KEY  (lihat output di bawah)")
    else:
        skip(".env sudah ada, tidak ditimpa.")


def step2_generate_keys():
    """Generate Airflow Fernet Key dan Secret Key."""
    try:
        from cryptography.fernet import Fernet
        fernet_key = Fernet.generate_key().decode()
    except ImportError:
        print("\n  [ERROR] Package 'cryptography' belum terinstall.")
        print("  Jalankan: pip install cryptography")
        sys.exit(1)

    secret_key = secrets.token_hex(32)

    print()
    divider()
    info("Salin dua baris berikut ke file .env:")
    divider()
    print(f"  {GREEN}AIRFLOW_FERNET_KEY={fernet_key}{RESET}")
    print(f"  {GREEN}AIRFLOW_SECRET_KEY={secret_key}{RESET}")
    divider()


def step3_check_env_values():
    """Cek apakah .env masih menggunakan nilai placeholder."""
    env_file = ROOT / ".env"
    if not env_file.exists():
        return

    placeholders = [
        "your_fernet_key_here",
        "your_webserver_secret_key_here",
        "your_telegram_bot_token",
        "your_chat_id",
    ]

    with open(env_file, "r") as f:
        content = f.read()

    found = [p for p in placeholders if p in content]
    if found:
        print()
        print(f"  {YELLOW}[WARN]{RESET} File .env masih memiliki nilai placeholder:")
        for p in found:
            print(f"         - {p}")
        print(f"  {YELLOW}       Pastikan diganti sebelum menjalankan `make up`.{RESET}")


def step4_check_requirements():
    """Cek apakah Docker dan Docker Compose tersedia."""
    print()
    import shutil as sh
    for tool in ["docker", "python"]:
        if sh.which(tool):
            ok(f"{tool} ditemukan")
        else:
            print(f"  {YELLOW}[WARN]{RESET} {tool} tidak ditemukan di PATH")


def step5_next_steps():
    """Tampilkan langkah berikutnya."""
    print()
    divider()
    info("Langkah berikutnya:")
    print(f"    1. Edit file .env  (isi AIRFLOW_FERNET_KEY dll dari output di atas)")
    print(f"    2. make up              (jalankan semua container)")
    print(f"    3. make ps              (pastikan semua container healthy)")
    print(f"    4. make airflow-init    (inisialisasi database Airflow)")
    print(f"    5. make verify          (verifikasi Fase 1)")
    divider()
    print()


def main():
    header("Bitcoin Volatility ML - Setup Awal")
    step1_copy_env()
    step2_generate_keys()
    step3_check_env_values()
    step4_check_requirements()
    step5_next_steps()


if __name__ == "__main__":
    main()