-- ANSI SQL dialect example - Products table
-- Target warehouse: SQLite (default)
-- Demonstrates standard SQL data types

CREATE TABLE products (
    product_id INTEGER NOT NULL,
    sku VARCHAR(100),
    product_name VARCHAR(255),
    description VARCHAR(1000),
    category VARCHAR(100),
    price REAL,
    in_stock BOOLEAN,
    created_at DATE
);
