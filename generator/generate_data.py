#!/usr/bin/env python3
"""
Continuous CDC workload generator for the inventory database.

Drives a realistic mix of INSERT / UPDATE / DELETE across customers, products,
orders, and order_items so Debezium has a steady stream of WAL changes to
capture. Runs forever until interrupted (Ctrl-C).

Connection and rate are configured via environment variables:

    PGHOST      (default: localhost)   use "postgres" when run inside compose
    PGPORT      (default: 5432)
    PGUSER      (default: cdc_admin)
    PGPASSWORD  (default: cdc_password)
    PGDATABASE  (default: inventory)
    OPS_PER_SEC (default: 5)           approximate write operations per second
    SEED_CUSTOMERS (default: 50)       extra customers inserted at startup
    SEED_PRODUCTS  (default: 30)       extra products inserted at startup

Run locally:
    pip install -r requirements.txt
    python generate_data.py
"""

import os
import random
import signal
import sys
import time

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

fake = Faker()

CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "user": os.getenv("PGUSER", "cdc_admin"),
    "password": os.getenv("PGPASSWORD", "cdc_password"),
    "dbname": os.getenv("PGDATABASE", "inventory"),
}
OPS_PER_SEC = float(os.getenv("OPS_PER_SEC", "5"))
SEED_CUSTOMERS = int(os.getenv("SEED_CUSTOMERS", "50"))
SEED_PRODUCTS = int(os.getenv("SEED_PRODUCTS", "30"))

ORDER_STATUSES = ["pending", "shipped", "delivered", "cancelled"]
CATEGORIES = ["Peripherals", "Displays", "Accessories", "Storage", "Networking"]

_running = True


def _stop(signum, frame):
    global _running
    _running = False
    print("\nShutting down generator...", flush=True)


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def connect():
    """Connect, retrying until Postgres is reachable."""
    while _running:
        try:
            conn = psycopg2.connect(**CONFIG)
            conn.autocommit = True
            print(f"Connected to {CONFIG['host']}:{CONFIG['port']}/{CONFIG['dbname']}", flush=True)
            return conn
        except psycopg2.OperationalError as exc:
            print(f"Postgres not ready ({exc}); retrying in 2s...", flush=True)
            time.sleep(2)
    sys.exit(0)


def seed(conn):
    """Insert a batch of extra customers and products at startup."""
    with conn.cursor() as cur:
        customers = [
            (fake.first_name(), fake.last_name(), fake.unique.email(), fake.phone_number()[:40])
            for _ in range(SEED_CUSTOMERS)
        ]
        execute_values(
            cur,
            "INSERT INTO customers (first_name, last_name, email, phone) VALUES %s "
            "ON CONFLICT (email) DO NOTHING",
            customers,
        )
        products = [
            (
                f"SKU-{random.randint(1000, 999999):06d}",
                f"{fake.color_name()} {fake.word().capitalize()}",
                random.choice(CATEGORIES),
                round(random.uniform(5, 999), 2),
                random.randint(0, 1000),
            )
            for _ in range(SEED_PRODUCTS)
        ]
        execute_values(
            cur,
            "INSERT INTO products (sku, name, category, price, stock_quantity) VALUES %s "
            "ON CONFLICT (sku) DO NOTHING",
            products,
        )
    print(f"Seeded ~{SEED_CUSTOMERS} customers and ~{SEED_PRODUCTS} products", flush=True)


def _ids(conn, table, pk):
    with conn.cursor() as cur:
        cur.execute(f"SELECT {pk} FROM {table}")
        return [row[0] for row in cur.fetchall()]


# --- individual write operations ------------------------------------------

def insert_customer(conn):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO customers (first_name, last_name, email, phone) VALUES (%s, %s, %s, %s)",
            (fake.first_name(), fake.last_name(), fake.unique.email(), fake.phone_number()[:40]),
        )


def insert_order(conn):
    customers = _ids(conn, "customers", "customer_id")
    products = _ids(conn, "products", "product_id")
    if not customers or not products:
        return
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO orders (customer_id, status, total_amount, order_date) "
            "VALUES (%s, 'pending', 0, now()) RETURNING order_id",
            (random.choice(customers),),
        )
        order_id = cur.fetchone()[0]
        total = 0.0
        for _ in range(random.randint(1, 4)):
            pid = random.choice(products)
            qty = random.randint(1, 5)
            cur.execute("SELECT price FROM products WHERE product_id = %s", (pid,))
            price = float(cur.fetchone()[0])
            total += price * qty
            cur.execute(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) "
                "VALUES (%s, %s, %s, %s)",
                (order_id, pid, qty, price),
            )
        cur.execute("UPDATE orders SET total_amount = %s WHERE order_id = %s", (round(total, 2), order_id))


def update_order_status(conn):
    orders = _ids(conn, "orders", "order_id")
    if not orders:
        return
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE orders SET status = %s, updated_at = now() WHERE order_id = %s",
            (random.choice(ORDER_STATUSES), random.choice(orders)),
        )


def update_product_stock(conn):
    products = _ids(conn, "products", "product_id")
    if not products:
        return
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE products SET stock_quantity = GREATEST(0, stock_quantity + %s), updated_at = now() "
            "WHERE product_id = %s",
            (random.randint(-20, 50), random.choice(products)),
        )


def delete_order(conn):
    """Cancel-and-remove: delete a delivered/cancelled order and its items."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT order_id FROM orders WHERE status IN ('delivered', 'cancelled') "
            "ORDER BY random() LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return
        order_id = row[0]
        cur.execute("DELETE FROM order_items WHERE order_id = %s", (order_id,))
        cur.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))


# Weighted operation mix: mostly orders/updates, fewer customers, occasional deletes.
OPERATIONS = (
    [insert_order] * 8
    + [update_order_status] * 6
    + [update_product_stock] * 4
    + [insert_customer] * 2
    + [delete_order] * 2
)


def main():
    conn = connect()
    seed(conn)
    print(f"Generating ~{OPS_PER_SEC} ops/sec. Press Ctrl-C to stop.", flush=True)

    delay = 1.0 / OPS_PER_SEC if OPS_PER_SEC > 0 else 0.2
    counts = {}
    last_report = time.time()

    while _running:
        op = random.choice(OPERATIONS)
        try:
            op(conn)
            counts[op.__name__] = counts.get(op.__name__, 0) + 1
        except psycopg2.Error as exc:
            print(f"Operation {op.__name__} failed: {exc}", flush=True)
            conn = connect()  # reconnect on error

        if time.time() - last_report >= 10:
            summary = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
            print(f"[{time.strftime('%H:%M:%S')}] ops so far: {summary}", flush=True)
            last_report = time.time()

        time.sleep(delay)

    conn.close()
    print("Generator stopped.", flush=True)


if __name__ == "__main__":
    main()
