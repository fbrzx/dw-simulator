-- Snowflake dialect example - Products table
-- Demonstrates Snowflake-specific data types

CREATE TABLE products (
    product_id NUMBER(10,0) NOT NULL,
    sku VARCHAR(100),
    product_name VARCHAR(255),
    description VARCHAR(1000),
    category VARCHAR(100),
    price NUMBER(10,2),
    in_stock BOOLEAN,
    created_at TIMESTAMP
);
