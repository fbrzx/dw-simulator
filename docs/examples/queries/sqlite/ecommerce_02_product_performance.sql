-- Product Performance Analysis
-- Analyzes product sales, revenue, and profitability

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.price as current_price,
    COUNT(DISTINCT oi.order_id) as times_ordered,
    SUM(oi.quantity) as total_units_sold,
    SUM(oi.quantity * oi.unit_price) as total_revenue,
    AVG(oi.unit_price) as avg_selling_price,
    COUNT(DISTINCT o.customer_id) as unique_customers
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
LEFT JOIN orders o ON oi.order_id = o.order_id
GROUP BY
    p.product_id,
    p.product_name,
    p.category,
    p.price
HAVING times_ordered > 0
ORDER BY total_revenue DESC
LIMIT 50;
