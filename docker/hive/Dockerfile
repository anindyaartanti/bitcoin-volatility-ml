FROM bitsondatadev/hive-metastore:latest

ENV PG_JDBC_VERSION=42.7.1

RUN curl -fsSL -o /opt/apache-hive-metastore-3.0.0-bin/lib/postgresql-jdbc.jar \
    "https://jdbc.postgresql.org/download/postgresql-${PG_JDBC_VERSION}.jar"

COPY hive/hive-site.xml /opt/apache-hive-metastore-3.0.0-bin/conf/

COPY --chmod=0755 docker/hive/entrypoint.sh /entrypoint.sh
