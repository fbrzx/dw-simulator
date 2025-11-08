-- Snowflake dialect example - Orders table
-- Target warehouse: Snowflake (LocalStack emulator)
-- Multi-table experiment example

CREATE TABLE orders (
    order_id NUMBER(10,0) NOT NULL,
    customer_id NUMBER(10,0),
    order_date DATE,
    total_amount NUMBER(10,2),
    status VARCHAR(50),
    shipping_address VARCHAR(500)
);

CREATE TABLE order_items (
    item_id NUMBER(10,0) NOT NULL,
    order_id NUMBER(10,0),
    product_name VARCHAR(255),
    quantity NUMBER(10,0),
    unit_price NUMBER(10,2)
);
