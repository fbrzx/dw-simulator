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
    o.shipping_cost,
    o.tax_amount,
    o.status,
    o.payment_method,
    GROUP_CONCAT(p.product_name, ', ') as products_ordered
FROM ecommerce_simple__orders o
JOIN ecommerce_simple__customers c ON o.customer_id = c.customer_id
LEFT JOIN ecommerce_simple__order_items oi ON o.order_id = oi.order_id
LEFT JOIN ecommerce_simple__products p ON oi.product_id = p.product_id
GROUP BY
    o.order_id,
    o.order_date,
    c.customer_id,
    c.email,
    o.total_amount,
    o.shipping_cost,
    o.tax_amount,
    o.status,
    o.payment_method
ORDER BY o.order_date DESC
LIMIT 100;
