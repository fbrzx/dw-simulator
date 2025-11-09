-- Monthly Revenue Trends
-- Tracks revenue, orders, and average order value by month

SELECT
    strftime('%Y-%m', o.order_date) as order_month,
    COUNT(DISTINCT o.order_id) as total_orders,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value,
    SUM(oi.quantity) as total_items_sold
FROM ecommerce_simple__orders o
LEFT JOIN ecommerce_simple__order_items oi ON o.order_id = oi.order_id
GROUP BY order_month
ORDER BY order_month;
