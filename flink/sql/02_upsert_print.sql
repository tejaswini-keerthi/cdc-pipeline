-- =============================================================================
-- STEP 4 job: upsert + fingerprint, sinking to `print` to demonstrate dedup.
-- (Step 5 replaces these print sinks with the Iceberg sink.)
--
-- Fingerprint columns:
--   pk_fingerprint      = MD5 of the primary key -> stable dedup/distribution key.
--                         Replayed events for the same PK hash identically, so the
--                         upsert sink collapses them onto one logical row.
--   content_fingerprint = MD5 of the business columns -> lets downstream (Step 7)
--                         detect no-op updates (same content, new event).
-- =============================================================================

-- ---- print sinks -----------------------------------------------------------
CREATE TABLE customers_out (
    customer_id INT,
    first_name  STRING,
    last_name   STRING,
    email       STRING,
    phone       STRING,
    pk_fingerprint      STRING,
    content_fingerprint STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH ('connector' = 'print', 'print-identifier' = 'CUSTOMERS');

CREATE TABLE products_out (
    product_id     INT,
    sku            STRING,
    name           STRING,
    category       STRING,
    price          DOUBLE,
    stock_quantity INT,
    pk_fingerprint      STRING,
    content_fingerprint STRING,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH ('connector' = 'print', 'print-identifier' = 'PRODUCTS');

CREATE TABLE orders_out (
    order_id     INT,
    customer_id  INT,
    status       STRING,
    total_amount DOUBLE,
    pk_fingerprint      STRING,
    content_fingerprint STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('connector' = 'print', 'print-identifier' = 'ORDERS');

CREATE TABLE order_items_out (
    order_item_id INT,
    order_id      INT,
    product_id    INT,
    quantity      INT,
    unit_price    DOUBLE,
    pk_fingerprint      STRING,
    content_fingerprint STRING,
    PRIMARY KEY (order_item_id) NOT ENFORCED
) WITH ('connector' = 'print', 'print-identifier' = 'ORDER_ITEMS');

-- ---- single job running all four upserts -----------------------------------
EXECUTE STATEMENT SET
BEGIN
    INSERT INTO customers_out
    SELECT
        customer_id, first_name, last_name, email, phone,
        MD5(CAST(customer_id AS STRING)) AS pk_fingerprint,
        MD5(CONCAT_WS('|',
            COALESCE(first_name, ''), COALESCE(last_name, ''),
            COALESCE(email, ''), COALESCE(phone, ''))) AS content_fingerprint
    FROM customers_src;

    INSERT INTO products_out
    SELECT
        product_id, sku, name, category, price, stock_quantity,
        MD5(CAST(product_id AS STRING)) AS pk_fingerprint,
        MD5(CONCAT_WS('|',
            COALESCE(sku, ''), COALESCE(name, ''), COALESCE(category, ''),
            CAST(price AS STRING), CAST(stock_quantity AS STRING))) AS content_fingerprint
    FROM products_src;

    INSERT INTO orders_out
    SELECT
        order_id, customer_id, status, total_amount,
        MD5(CAST(order_id AS STRING)) AS pk_fingerprint,
        MD5(CONCAT_WS('|',
            CAST(customer_id AS STRING), COALESCE(status, ''),
            CAST(total_amount AS STRING))) AS content_fingerprint
    FROM orders_src;

    INSERT INTO order_items_out
    SELECT
        order_item_id, order_id, product_id, quantity, unit_price,
        MD5(CAST(order_item_id AS STRING)) AS pk_fingerprint,
        MD5(CONCAT_WS('|',
            CAST(order_id AS STRING), CAST(product_id AS STRING),
            CAST(quantity AS STRING), CAST(unit_price AS STRING))) AS content_fingerprint
    FROM order_items_src;
END;
