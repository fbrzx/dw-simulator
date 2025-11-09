-- Customer Cohort Analysis
-- Groups customers by signup month and tracks their ordering behavior

SELECT
    strftime('%Y-%m', c.signup_date) as cohort_month,
    COUNT(DISTINCT c.customer_id) as total_customers,
    COUNT(DISTINCT CASE WHEN o.order_id IS NOT NULL THEN c.customer_id END) as customers_with_orders,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN o.order_id IS NOT NULL THEN c.customer_id END) / COUNT(DISTINCT c.customer_id), 2) as activation_rate,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_revenue,
    AVG(CASE WHEN o.order_id IS NOT NULL THEN o.total_amount END) as avg_order_value
FROM ecommerce_simple__customers c
LEFT JOIN ecommerce_simple__orders o ON c.customer_id = o.customer_id
GROUP BY cohort_month
ORDER BY cohort_month;
