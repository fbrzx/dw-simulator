-- Redshift dialect example - Products table
-- Target warehouse: Redshift (PostgreSQL emulator)
-- Demonstrates Redshift-specific data types

CREATE TABLE products (
    product_id INT NOT NULL,
    sku VARCHAR(100),
    product_name VARCHAR(255),
    description VARCHAR(1000),
    category VARCHAR(100),
    price FLOAT,
    in_stock BOOLEAN,
    created_at TIMESTAMP
);
