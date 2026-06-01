-- =============================================================================
-- CDC source tables. Each reads one Debezium topic with format 'debezium-json',
-- which decodes the before/after/op envelope into a Flink changelog.
--
--   PRIMARY KEY ... NOT ENFORCED  -> upsert stream keyed by PK. This is the
--   deduplication mechanism: replayed/at-least-once events with the same key are
--   idempotent and converge to a single row.
--
--   'debezium-json.schema-include' = 'true' matches our connector setting
--   value.converter.schemas.enable=true (messages are {schema, payload}).
--
-- Type mapping notes:
--   NUMERIC -> DOUBLE      (connector decimal.handling.mode = double)
--   timestamptz -> TIMESTAMP_LTZ(3)  (Debezium emits ISO-8601 / ZonedTimestamp)
--   ingest_ts is the Kafka record timestamp, used for latency metrics later.
-- =============================================================================

CREATE TABLE customers_src (
    customer_id INT,
    first_name  STRING,
    last_name   STRING,
    email       STRING,
    phone       STRING,
    created_at  TIMESTAMP_LTZ(3),
    updated_at  TIMESTAMP_LTZ(3),
    ingest_ts   TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'inventory.public.customers',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-cdc-customers',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'debezium-json.schema-include' = 'true',
    'debezium-json.ignore-parse-errors' = 'true'
);

CREATE TABLE products_src (
    product_id     INT,
    sku            STRING,
    name           STRING,
    category       STRING,
    price          DOUBLE,
    stock_quantity INT,
    created_at     TIMESTAMP_LTZ(3),
    updated_at     TIMESTAMP_LTZ(3),
    ingest_ts      TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'inventory.public.products',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-cdc-products',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'debezium-json.schema-include' = 'true',
    'debezium-json.ignore-parse-errors' = 'true'
);

CREATE TABLE orders_src (
    order_id     INT,
    customer_id  INT,
    status       STRING,
    total_amount DOUBLE,
    order_date   TIMESTAMP_LTZ(3),
    updated_at   TIMESTAMP_LTZ(3),
    ingest_ts    TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'inventory.public.orders',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-cdc-orders',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'debezium-json.schema-include' = 'true',
    'debezium-json.ignore-parse-errors' = 'true'
);

CREATE TABLE order_items_src (
    order_item_id INT,
    order_id      INT,
    product_id    INT,
    quantity      INT,
    unit_price    DOUBLE,
    ingest_ts     TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    PRIMARY KEY (order_item_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'inventory.public.order_items',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-cdc-order-items',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'debezium-json.schema-include' = 'true',
    'debezium-json.ignore-parse-errors' = 'true'
);
