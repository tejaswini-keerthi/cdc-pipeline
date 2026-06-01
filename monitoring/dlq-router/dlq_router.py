#!/usr/bin/env python3
"""
Dead Letter Queue router for the CDC stream.

Consumes the Debezium CDC topics, validates each change event, and routes any
malformed event to the DLQ topic (default: dlq.inventory) with error-reason
headers. Valid events are left untouched (Flink consumes them normally).

Exposes Prometheus metrics on :8001/metrics:
    cdc_dlq_events_total{table=...,reason=...}   Counter — events sent to the DLQ
    cdc_events_validated_total{table=...}        Counter — events that passed
    cdc_dlq_router_up                            Gauge   — 1 while running

Validation reasons:
    invalid_json     value is not parseable JSON
    missing_payload  no 'payload' object (not a Debezium envelope)
    bad_op           op not in {c, u, d, r, t}
    missing_table    no source.table

Uses its own consumer group at 'latest' so it only inspects live traffic.
"""

import json
import os
import time

from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, Gauge, start_http_server

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_PATTERN = os.getenv("TOPIC_PATTERN", r"inventory\.public\..*")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dlq.inventory")
PORT = int(os.getenv("EXPORTER_PORT", "8001"))
GROUP_ID = os.getenv("GROUP_ID", "cdc-dlq-router")

VALID_OPS = {"c", "u", "d", "r", "t"}  # create, update, delete, read(snapshot), truncate

DLQ_EVENTS = Counter(
    "cdc_dlq_events_total",
    "Events routed to the DLQ, by table and failure reason",
    ["table", "reason"],
)
VALIDATED = Counter(
    "cdc_events_validated_total",
    "Events that passed validation, by table",
    ["table"],
)
UP = Gauge("cdc_dlq_router_up", "1 while the DLQ router is running")


def validate(value_bytes):
    """Return (table, reason). reason is None when the event is valid."""
    try:
        envelope = json.loads(value_bytes)
    except (ValueError, TypeError):
        return "unknown", "invalid_json"

    payload = envelope.get("payload") if isinstance(envelope, dict) else None
    # schemas.enable=true wraps in {schema,payload}; tolerate unwrapped too.
    if payload is None:
        if isinstance(envelope, dict) and "op" in envelope:
            payload = envelope
        else:
            return "unknown", "missing_payload"

    source = payload.get("source") or {}
    table = source.get("table")
    if not table:
        return "unknown", "missing_table"
    if payload.get("op") not in VALID_OPS:
        return table, "bad_op"
    return table, None


def build_clients():
    while True:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="latest",
                enable_auto_commit=True,
            )
            consumer.subscribe(pattern=TOPIC_PATTERN)
            producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
            print(f"DLQ router watching /{TOPIC_PATTERN}/ -> {DLQ_TOPIC}", flush=True)
            return consumer, producer
        except Exception as exc:  # noqa: BLE001 - retry on any connection error
            print(f"Kafka not ready ({exc}); retrying in 3s...", flush=True)
            time.sleep(3)


def main():
    start_http_server(PORT)
    UP.set(1)
    print(f"Metrics on :{PORT}/metrics", flush=True)
    consumer, producer = build_clients()

    for msg in consumer:
        if msg.value is None:
            continue  # delete tombstone — legitimately empty
        table, reason = validate(msg.value)
        if reason is None:
            VALIDATED.labels(table=table).inc()
            continue

        # Route the original (untouched) bytes to the DLQ with diagnostic headers.
        headers = [
            ("error_reason", reason.encode()),
            ("source_topic", (msg.topic or "").encode()),
            ("source_partition", str(msg.partition).encode()),
            ("source_offset", str(msg.offset).encode()),
        ]
        producer.send(DLQ_TOPIC, value=msg.value, headers=headers)
        DLQ_EVENTS.labels(table=table, reason=reason).inc()
        print(f"DLQ <- topic={msg.topic} offset={msg.offset} reason={reason}", flush=True)


if __name__ == "__main__":
    main()
