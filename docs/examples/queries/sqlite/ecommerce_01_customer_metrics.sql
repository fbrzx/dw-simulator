-- Customer Lifetime Value and Purchase Behavior
-- This query calculates key customer metrics including total spend, order count, and average order value

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.signup_date,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.total_amount) as lifetime_value,
    AVG(o.total_amount) as avg_order_value,
    MIN(o.order_date) as first_order_date,
    MAX(o.order_date) as last_order_date,
    COUNT(DISTINCT oi.product_id) as unique_products_purchased
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.signup_date
HAVING total_orders > 0
ORDER BY lifetime_value DESC
LIMIT 100;
