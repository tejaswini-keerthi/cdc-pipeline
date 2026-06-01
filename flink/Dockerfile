# Custom Flink image: stock Flink + SQL connector jars the pipeline needs.
# Base is pinned; connector versions are pinned via build args.
FROM flink:1.18.1-scala_2.12-java11

# Kafka SQL connector for the `debezium-json` CDC sources (Step 4).
ARG KAFKA_CONNECTOR_VERSION=3.1.0-1.18

# Iceberg sink (Step 5): Flink runtime + AWS bundle (S3FileIO), Postgres JDBC
# driver (JDBC catalog), and shaded Hadoop (Iceberg needs hadoop Configuration).
ARG ICEBERG_VERSION=1.5.2
ARG FLINK_MINOR=1.18
ARG PG_JDBC_VERSION=42.7.3
ARG HADOOP_UBER_VERSION=2.8.3-10.0

RUN set -eux; \
    cd /opt/flink/lib; \
    wget -q -O flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"; \
    wget -q -O iceberg-flink-runtime-${FLINK_MINOR}-${ICEBERG_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_MINOR}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_MINOR}-${ICEBERG_VERSION}.jar"; \
    wget -q -O iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    wget -q -O postgresql-${PG_JDBC_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"; \
    wget -q -O flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_UBER_VERSION}/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar"

# Note: the Prometheus metrics reporter already ships as a plugin in Flink 1.18
# (/opt/flink/plugins/metrics-prometheus/), so no staging step is needed — the
# reporter is enabled via metrics.reporter.* in FLINK_PROPERTIES (docker-compose).
