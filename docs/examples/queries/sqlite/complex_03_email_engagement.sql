-- Email Engagement Analysis
-- Analyzes email opt-in/opt-out patterns and customer email status
-- Data source: sas_email_addresses_vw, sas_customer_mstr_vw

SELECT
    CASE
        WHEN e.opt_out_date IS NOT NULL THEN 'Opted Out'
        WHEN e.opt_in_date IS NOT NULL THEN 'Active Subscriber'
        ELSE 'No Opt-In'
    END as email_status,
    COUNT(DISTINCT e.email_id) as email_count,
    COUNT(DISTINCT e.customer_id) as customer_count,
    COUNT(DISTINCT c.mstr_household_id) as household_count,
    ROUND(AVG(c.lifetime_cust_profit), 2) as avg_customer_lifetime_value,
    ROUND(AVG(c.m0_12_net), 2) as avg_12m_net_profit
FROM
    sas_email_addresses_vw e
INNER JOIN
    sas_customer_mstr_vw c ON e.customer_id = c.customer_id
GROUP BY
    email_status
ORDER BY
    email_count DESC;
