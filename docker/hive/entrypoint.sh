#!/bin/sh


set -e

export HADOOP_HOME=/opt/hadoop-3.2.0
export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar
export JAVA_HOME=/usr/local/openjdk-8

METASTORE_DB_HOSTNAME=${METASTORE_DB_HOSTNAME:-postgres}
METASTORE_DB_PORT=${METASTORE_DB_PORT:-5432}

echo "Waiting for PostgreSQL on ${METASTORE_DB_HOSTNAME}:${METASTORE_DB_PORT}..."
while ! nc -z ${METASTORE_DB_HOSTNAME} ${METASTORE_DB_PORT}; do
  sleep 1
done
echo "PostgreSQL ready."

echo "Initializing Hive Metastore schema (PostgreSQL)..."
/opt/apache-hive-metastore-3.0.0-bin/bin/schematool -initSchema -dbType postgres 2>/dev/null || true

echo "Starting Hive Metastore..."
exec /opt/apache-hive-metastore-3.0.0-bin/bin/start-metastore
