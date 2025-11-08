-- Redshift dialect example
-- Target warehouse: Redshift (PostgreSQL emulator)
-- Based on the sample customers_experiment schema

CREATE TABLE customers (
    customer_id INT NOT NULL,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    created_at DATE,
    is_active BOOLEAN
);
