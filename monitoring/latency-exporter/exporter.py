#!/usr/bin/env python3
"""
Per-table CDC replication-latency and throughput exporter for Prometheus.

Consumes the Debezium CDC topics and, for every change event, reads the source
commit timestamp (payload.source.ts_ms) and compares it to now. That difference
is the DB-commit -> capture replication latency, recorded as a per-table
histogram. It also counts events per table and per op (c/u/d/r) for throughput.

Exposed on :8000/metrics:
    cdc_replication_latency_seconds{table=...}   Histogram
    cdc_events_total{table=...,op=...}           Counter
    cdc_last_event_timestamp_seconds{table=...}  Gauge

Note: latency is measured at Kafka-consume time, so it captures the
Postgres -> Debezium -> Kafka path (the replication latency). It uses its own
consumer group and starts at 'latest' so it only reports live lag.
"""

import json
import os
import time

from kafka import KafkaConsumer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_PATTERN = os.getenv("TOPIC_PATTERN", r"inventory\.public\..*")
PORT = int(os.getenv("EXPORTER_PORT", "8000"))
GROUP_ID = os.getenv("GROUP_ID", "cdc-latency-exporter")

LATENCY = Histogram(
    "cdc_replication_latency_seconds",
    "DB-commit to capture (Kafka) replication latency, per table",
    ["table"],
    buckets=(0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 10, 30),
)
EVENTS = Counter(
    "cdc_events_total",
    "CDC change events observed, per table and op (c/u/d/r)",
    ["table", "op"],
)
LAST_EVENT = Gauge(
    "cdc_last_event_timestamp_seconds",
    "Unix timestamp of the last event seen, per table",
    ["table"],
)


def build_consumer():
    """Create the consumer, retrying until Kafka is reachable."""
    while True:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                # No consumer_timeout_ms: block indefinitely waiting for events.
                # (timeout=0 would stop the iterator immediately and exit.)
            )
            consumer.subscribe(pattern=TOPIC_PATTERN)
            print(f"Subscribed to /{TOPIC_PATTERN}/ on {BOOTSTRAP}", flush=True)
            return consumer
        except Exception as exc:  # noqa: BLE001 - retry on any connection error
            print(f"Kafka not ready ({exc}); retrying in 3s...", flush=True)
            time.sleep(3)


def main():
    start_http_server(PORT)
    print(f"Metrics exposed on :{PORT}/metrics", flush=True)
    consumer = build_consumer()

    for msg in consumer:
        if msg.value is None:
            continue  # delete tombstone — no payload to measure
        try:
            envelope = json.loads(msg.value)
            # value.converter.schemas.enable=true -> {"schema":..., "payload":...}
            payload = envelope.get("payload", envelope)
            source = payload.get("source") or {}
            table = source.get("table", "unknown")
            op = payload.get("op", "?")

            EVENTS.labels(table=table, op=op).inc()
            LAST_EVENT.labels(table=table).set(time.time())

            src_ts_ms = source.get("ts_ms")
            if src_ts_ms is not None:
                latency_s = max(0.0, (time.time() * 1000.0 - src_ts_ms) / 1000.0)
                LATENCY.labels(table=table).observe(latency_s)
        except (ValueError, AttributeError):
            continue  # skip anything that isn't a parseable Debezium event


if __name__ == "__main__":
    main()
