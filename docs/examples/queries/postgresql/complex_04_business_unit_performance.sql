-- Business Unit Performance Analysis
-- Analyzes transaction performance across different business units
-- Data source: sas_transaction_vw, sas_lookup_vw

SELECT
    t.business_unit,
    l.code_description as business_unit_name,
    COUNT(DISTINCT t.transaction_no) as transaction_count,
    COUNT(DISTINCT t.mstr_customer_id) as unique_customers,
    SUM(t.amount) as total_revenue,
    AVG(t.amount) as avg_transaction_value,
    MIN(t.amount) as min_transaction,
    MAX(t.amount) as max_transaction,
    ROUND((SUM(t.amount) / NULLIF(COUNT(DISTINCT t.mstr_customer_id), 0))::numeric, 2) as revenue_per_customer
FROM
    sas_transaction_vw t
LEFT JOIN
    sas_lookup_vw l ON t.business_unit = l.code
WHERE
    l.code_id IS NULL OR l.code_id IN (SELECT code_id FROM sas_lookup_vw WHERE code = t.business_unit LIMIT 1)
GROUP BY
    t.business_unit,
    l.code_description
ORDER BY
    total_revenue DESC;
