-- ANSI SQL dialect example - Orders table
-- Target warehouse: SQLite (default)
-- Multi-table experiment example

CREATE TABLE orders (
    order_id INTEGER NOT NULL,
    customer_id INTEGER,
    order_date DATE,
    total_amount REAL,
    status VARCHAR(50),
    shipping_address VARCHAR(500)
);

CREATE TABLE order_items (
    item_id INTEGER NOT NULL,
    order_id INTEGER,
    product_name VARCHAR(255),
    quantity INTEGER,
    unit_price REAL
);
