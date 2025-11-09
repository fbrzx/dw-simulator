-- Monthly Revenue Trends
-- Tracks revenue, orders, and average order value by month
-- PostgreSQL/Redshift version: uses to_char() instead of strftime()

SELECT
    to_char(o.order_date, 'YYYY-MM') as order_month,
    COUNT(DISTINCT o.order_id) as total_orders,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value,
    SUM(oi.quantity) as total_items_sold
FROM orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY order_month
ORDER BY order_month;
