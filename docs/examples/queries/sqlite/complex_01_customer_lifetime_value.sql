-- Customer Lifetime Value Analysis
-- Analyzes customer profitability and transaction behavior
-- Data source: sas_customer_mstr_vw, sas_transaction_vw

SELECT
    c.mstr_customer_id,
    c.mstr_household_id,
    COUNT(DISTINCT t.transaction_no) as total_transactions,
    SUM(t.amount) as total_transaction_amount,
    AVG(t.amount) as avg_transaction_amount,
    MAX(t.amount) as max_transaction_amount,
    c.lifetime_cust_profit,
    c.m0_12_net as last_12m_net_profit,
    ROUND(c.lifetime_cust_profit / NULLIF(COUNT(DISTINCT t.transaction_no), 0), 2) as profit_per_transaction,
    COUNT(DISTINCT t.business_unit) as business_units_purchased_from
FROM
    sas_customer_mstr_vw c
LEFT JOIN
    sas_transaction_vw t ON c.mstr_customer_id = t.mstr_customer_id
GROUP BY
    c.mstr_customer_id,
    c.mstr_household_id,
    c.lifetime_cust_profit,
    c.m0_12_net
ORDER BY
    c.lifetime_cust_profit DESC
LIMIT 100;
