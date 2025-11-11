-- Clickstream Customer Behavior Analysis
-- Analyzes online behavior patterns and correlates with customer value
-- Data source: clickstream_aggregate_vw, sas_customer_mstr_vw

SELECT
    CASE
        WHEN ca.total_visits >= 50 THEN 'Heavy User'
        WHEN ca.total_visits >= 20 THEN 'Regular User'
        WHEN ca.total_visits >= 5 THEN 'Occasional User'
        ELSE 'Light User'
    END as user_segment,
    COUNT(DISTINCT ca.mstr_customer_id) as customer_count,
    AVG(ca.total_visits) as avg_total_visits,
    AVG(ca.avg_total_pv_visits) as avg_page_views_per_visit,
    AVG(c.lifetime_cust_profit) as avg_lifetime_value,
    AVG(c.m0_12_net) as avg_12m_net_profit,
    SUM(CASE WHEN c.lifetime_cust_profit > 0 THEN 1 ELSE 0 END) as profitable_customers,
    ROUND((100.0 * SUM(CASE WHEN c.lifetime_cust_profit > 0 THEN 1 ELSE 0 END) / COUNT(*))::numeric, 2) as profitable_customer_pct
FROM
    clickstream_aggregate_vw ca
INNER JOIN
    sas_customer_mstr_vw c ON ca.mstr_customer_id = c.mstr_customer_id
GROUP BY
    user_segment
ORDER BY
    avg_total_visits DESC;
