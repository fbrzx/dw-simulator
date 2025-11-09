-- Shopping Cart and Order Composition Analysis
-- Analyzes items per order, popular product combinations

SELECT
    o.order_id,
    o.order_date,
    c.customer_id,
    c.email,
    COUNT(oi.item_id) as items_in_order,
    SUM(oi.quantity) as total_quantity,
    SUM(oi.quantity * oi.unit_price) as items_total,
    o.total_amount as order_total,
    GROUP_CONCAT(p.product_name, ', ') as products_ordered
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
LEFT JOIN products p ON oi.product_id = p.product_id
GROUP BY
    o.order_id,
    o.order_date,
    c.customer_id,
    c.email,
    o.total_amount
ORDER BY o.order_date DESC
LIMIT 100;
