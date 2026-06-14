FROM bitsondatadev/hive-metastore:latest

ENV PG_JDBC_VERSION=42.7.1

# Download PostgreSQL JDBC driver
RUN curl -fsSL -o /opt/apache-hive-metastore-3.0.0-bin/lib/postgresql-jdbc.jar \
    "https://jdbc.postgresql.org/download/postgresql-${PG_JDBC_VERSION}.jar"

# Copy PostgreSQL-specific hive-site.xml
COPY hive/hive-site.xml /opt/apache-hive-metastore-3.0.0-bin/conf/

# Override entrypoint with PostgreSQL-aware version
COPY --chmod=0755 docker/hive/entrypoint.sh /entrypoint.sh
