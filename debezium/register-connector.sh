#!/usr/bin/env bash
# Registers (or updates) the Debezium Postgres connector via the Connect REST API.
# Loads creds from ../.env, substitutes ${VAR} placeholders, then PUTs the config
# (idempotent create-or-update).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG_FILE="$SCRIPT_DIR/postgres-connector.json"
ENV_FILE="$SCRIPT_DIR/../.env"

# Load .env so $POSTGRES_USER etc. are available to envsubst.
if [[ -f "$ENV_FILE" ]]; then
  set -a; source "$ENV_FILE"; set +a
  echo "Loaded env from $ENV_FILE"
fi

# Substitute placeholders.
CONFIG_JSON="$(envsubst < "$CONFIG_FILE")"
NAME="$(echo "$CONFIG_JSON" | jq -r '.name')"
CONFIG_BODY="$(echo "$CONFIG_JSON" | jq '.config')"

echo "Waiting for Kafka Connect at $CONNECT_URL ..."
for _ in $(seq 1 30); do
  if curl -sf "$CONNECT_URL/connectors" >/dev/null; then break; fi
  sleep 2
done
echo "Kafka Connect is up."

echo "Registering connector '$NAME' ..."
curl -sf -X PUT -H "Content-Type: application/json" \
  --data "$CONFIG_BODY" \
  "$CONNECT_URL/connectors/$NAME/config" >/dev/null

sleep 2
echo ""
curl -sf "$CONNECT_URL/connectors/$NAME/status" | jq .
echo ""
echo "Topics will appear as: inventory.public.<table>"
