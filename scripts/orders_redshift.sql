-- Redshift dialect example - Orders table
-- Multi-table experiment example

CREATE TABLE orders (
    order_id INT NOT NULL,
    customer_id INT,
    order_date DATE,
    total_amount FLOAT,
    status VARCHAR(50),
    shipping_address VARCHAR(500)
);

CREATE TABLE order_items (
    item_id INT NOT NULL,
    order_id INT,
    product_name VARCHAR(255),
    quantity INT,
    unit_price FLOAT
);
