#!/usr/bin/env bash
# Renders the Iceberg sink SQL from .env and submits it to Flink (exactly-once).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER="${CONTAINER:-cdc-flink-jobmanager}"
TEMPLATE="$SCRIPT_DIR/sql/03_iceberg_sink.sql.tmpl"
ENV_FILE="$SCRIPT_DIR/../.env"

[[ -f "$ENV_FILE" ]] || { echo "$ENV_FILE not found"; exit 1; }
set -a; source "$ENV_FILE"; set +a

mkdir -p "$SCRIPT_DIR/sql/.gen"
GEN_FILE="$SCRIPT_DIR/sql/.gen/03_iceberg_sink.sql"
envsubst < "$TEMPLATE" > "$GEN_FILE"
echo "Rendered Iceberg job -> $GEN_FILE"

echo "Submitting Iceberg sink job via sql-client in $CONTAINER ..."
docker exec "$CONTAINER" /opt/flink/bin/sql-client.sh \
  -i /opt/flink/sql/00_settings.sql \
  -i /opt/flink/sql/01_sources.sql \
  -f /opt/flink/sql/.gen/03_iceberg_sink.sql

echo ""
echo "Iceberg sink job submitted. Watch it at http://localhost:8081"
echo "Data lands in MinIO bucket 'warehouse' (console: http://localhost:9001)."
