#!/usr/bin/env bash
# Submits the Flink CDC upsert/dedup job via the SQL Client in the JobManager.
# Settings + source DDL are init scripts (-i); the job is the -f script.
set -euo pipefail

CONTAINER="${CONTAINER:-cdc-flink-jobmanager}"
JOB_FILE="${1:-02_upsert_print.sql}"

echo "Submitting Flink job '$JOB_FILE' via sql-client in $CONTAINER ..."
docker exec "$CONTAINER" /opt/flink/bin/sql-client.sh \
  -i /opt/flink/sql/00_settings.sql \
  -i /opt/flink/sql/01_sources.sql \
  -f "/opt/flink/sql/$JOB_FILE"

echo ""
echo "Job submitted. Check it at http://localhost:8081 (Running Jobs)."
echo "Print output appears in: docker logs -f cdc-flink-taskmanager"
