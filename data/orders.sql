-- Demo order history for the Shopio support agent.
-- support_agent/orders.py builds data/orders.db from this file on first use.
-- Delete data/orders.db to reset (refunds are written to the db, not here).

CREATE TABLE orders (
    order_id        TEXT PRIMARY KEY,
    order_date      TEXT NOT NULL,          -- YYYY-MM-DD
    status          TEXT NOT NULL,          -- shipping | delivered | cancelled
    total           REAL NOT NULL,          -- baht
    refund_id       TEXT,                   -- set when refunded
    refunded_amount REAL,
    refunded_at     TEXT
);

CREATE TABLE order_items (
    order_id TEXT NOT NULL REFERENCES orders(order_id),
    name     TEXT NOT NULL,
    qty      INTEGER NOT NULL,
    price    REAL NOT NULL                  -- per unit
);

INSERT INTO orders (order_id, order_date, status, total) VALUES
    ('SO-10512', '2026-11-18', 'shipping',  650),
    ('SO-10482', '2026-11-15', 'delivered', 890),
    ('SO-10457', '2026-11-10', 'delivered',  99),
    ('SO-10431', '2026-11-02', 'delivered', 450),
    ('SO-10398', '2026-10-23', 'delivered',  79),
    ('SO-10377', '2026-10-12', 'delivered', 1290),
    ('SO-10350', '2026-09-28', 'cancelled', 320);

INSERT INTO order_items (order_id, name, qty, price) VALUES
    ('SO-10512', 'LED desk lamp',          1, 650),
    ('SO-10482', 'Wireless earbuds',       1, 890),
    ('SO-10457', 'Phone case',             1,  59),
    ('SO-10457', 'Screen protector',       1,  40),
    ('SO-10431', 'Cotton T-shirt (M)',     2, 225),
    ('SO-10398', 'Ceramic coffee mug',     1,  79),
    ('SO-10377', 'Running shoes (42)',     1, 1290),
    ('SO-10350', 'Yoga mat',               1, 320);
