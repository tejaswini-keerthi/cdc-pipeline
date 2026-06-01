-- =============================================================================
-- CDC source schema: inventory / orders
-- Runs automatically on first Postgres init (docker-entrypoint-initdb.d).
-- Idempotent (IF NOT EXISTS) so it is safe to re-apply manually.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- customers
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    customer_id  SERIAL PRIMARY KEY,
    first_name   VARCHAR(100)        NOT NULL,
    last_name    VARCHAR(100)        NOT NULL,
    email        VARCHAR(255) UNIQUE NOT NULL,
    phone        VARCHAR(40),
    created_at   TIMESTAMPTZ         NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ         NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- products
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS products (
    product_id     SERIAL PRIMARY KEY,
    sku            VARCHAR(64) UNIQUE NOT NULL,
    name           VARCHAR(255)       NOT NULL,
    category       VARCHAR(100),
    price          NUMERIC(10,2)      NOT NULL,
    stock_quantity INTEGER            NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ        NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ        NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- orders
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    order_id      SERIAL PRIMARY KEY,
    customer_id   INTEGER       NOT NULL REFERENCES customers(customer_id),
    status        VARCHAR(20)   NOT NULL DEFAULT 'pending',  -- pending|shipped|delivered|cancelled
    total_amount  NUMERIC(12,2) NOT NULL DEFAULT 0,
    order_date    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- order_items
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id      INTEGER       NOT NULL REFERENCES orders(order_id),
    product_id    INTEGER       NOT NULL REFERENCES products(product_id),
    quantity      INTEGER       NOT NULL DEFAULT 1,
    unit_price    NUMERIC(10,2) NOT NULL DEFAULT 0
);

-- ---------------------------------------------------------------------------
-- CDC configuration
-- REPLICA IDENTITY FULL => WAL carries the full "before" image of each row,
-- so Debezium emits complete UPDATE/DELETE events (needed for dedup + DLQ).
-- ---------------------------------------------------------------------------
ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE products    REPLICA IDENTITY FULL;
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

-- Publication used by the Debezium pgoutput plugin to stream changes.
-- DROP first so re-running this script keeps it in sync.
DROP PUBLICATION IF EXISTS dbz_publication;
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- Helpful indexes for the foreign keys (also speeds up the data generator).
CREATE INDEX IF NOT EXISTS idx_orders_customer_id   ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id  ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);
