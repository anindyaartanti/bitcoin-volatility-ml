#!/bin/bash
# security/postgres/create_multiple_db.sh
# Membuat beberapa database sekaligus saat PostgreSQL container pertama start
# Dipanggil sebelum init.sql (prefix 00_)

set -e

# POSTGRES_MULTIPLE_DATABASES diisi dari .env, dipisah koma
# Contoh: btcdb,airflowdb,mlflowdb

create_user_and_database() {
    local database=$1
    echo ">>> Membuat database: $database"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        SELECT 'CREATE DATABASE $database'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$database')\gexec
        GRANT ALL PRIVILEGES ON DATABASE $database TO $POSTGRES_USER;
EOSQL
}

if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "=== Inisialisasi multiple databases: $POSTGRES_MULTIPLE_DATABASES ==="
    for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
        create_user_and_database $db
    done
    echo "=== Semua database berhasil dibuat ==="
fi
