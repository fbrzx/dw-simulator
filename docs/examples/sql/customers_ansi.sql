-- ANSI SQL dialect example
-- Target warehouse: SQLite (default)
-- Based on the sample customers_experiment schema

CREATE TABLE customers (
    customer_id INTEGER NOT NULL,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    created_at DATE,
    is_active BOOLEAN
);
