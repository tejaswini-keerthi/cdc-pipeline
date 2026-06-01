#!/usr/bin/env bash
# Demonstrates the DLQ + alerting loop by injecting malformed events.
set -euo pipefail

TOPIC="${1:-inventory.public.orders}"
COUNT="${2:-3}"

echo "Producing $COUNT malformed record(s) to $TOPIC ..."
for i in $(seq 1 "$COUNT"); do
  echo "this-is-not-valid-debezium-json-$i"
done | docker exec -i cdc-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 --topic "$TOPIC"

cat <<'EOF'

Injected. Now observe the DLQ loop:
  1. DLQ router routed them:   docker logs --tail 20 cdc-dlq-router
  2. DLQ topic contents:
       docker exec cdc-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
         --topic dlq.inventory --from-beginning --timeout-ms 5000
  3. Metric:                   curl -s http://localhost:8001/metrics | grep cdc_dlq_events_total
  4. Alert (after ~10-15s):    docker logs --tail 20 cdc-alert-webhook
     or the Alertmanager UI:   http://localhost:9093
EOF
