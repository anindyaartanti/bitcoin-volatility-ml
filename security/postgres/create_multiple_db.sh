#!/bin/bash




set -e




create_user_and_database() {
    local database=$1
    echo ">>> Membuat database: $database"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        SELECT 'CREATE DATABASE $database'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$database')\gexec
        GRANT ALL PRIVILEGES ON DATABASE $database TO $POSTGRES_USER;
EOSQL
}


create_marquez_user() {
    echo ">>> Membuat marquez user & database"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        DO \$\$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'marquez') THEN
                CREATE ROLE marquez WITH LOGIN PASSWORD 'k4ipbd_marquez_2026';
            END IF;
        END\$\$;
        SELECT 'CREATE DATABASE marquezdb'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'marquezdb')\gexec
        GRANT ALL PRIVILEGES ON DATABASE marquezdb TO marquez;
EOSQL
}

if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "=== Inisialisasi multiple databases: $POSTGRES_MULTIPLE_DATABASES ==="
    for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
        create_user_and_database $db
    done
    echo "=== Semua database berhasil dibuat ==="
fi


create_marquez_user
