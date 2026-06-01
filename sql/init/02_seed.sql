-- =============================================================================
-- Minimal seed data so the generator has customers/products to reference
-- immediately. The generator adds far more at runtime.
-- =============================================================================

INSERT INTO customers (first_name, last_name, email, phone) VALUES
    ('Ada',     'Lovelace',  'ada@example.com',     '555-0101'),
    ('Alan',    'Turing',    'alan@example.com',    '555-0102'),
    ('Grace',   'Hopper',    'grace@example.com',   '555-0103'),
    ('Linus',   'Torvalds',  'linus@example.com',   '555-0104'),
    ('Margaret','Hamilton',  'margaret@example.com','555-0105')
ON CONFLICT (email) DO NOTHING;

INSERT INTO products (sku, name, category, price, stock_quantity) VALUES
    ('SKU-0001', 'Mechanical Keyboard', 'Peripherals', 89.99,  500),
    ('SKU-0002', 'Wireless Mouse',      'Peripherals', 39.99,  800),
    ('SKU-0003', '27in Monitor',        'Displays',    299.99, 200),
    ('SKU-0004', 'USB-C Hub',           'Accessories', 49.99,  650),
    ('SKU-0005', 'Laptop Stand',        'Accessories', 29.99,  400)
ON CONFLICT (sku) DO NOTHING;
