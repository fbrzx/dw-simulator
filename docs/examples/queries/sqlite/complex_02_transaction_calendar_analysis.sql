-- Transaction Calendar Analysis
-- Analyzes transaction patterns by fiscal calendar periods
-- Data source: sas_transaction_vw, sas_calendar

SELECT
    cal.fyr as fiscal_year,
    cal.fmm as fiscal_month,
    cal.cycq as cycle_quarter,
    COUNT(DISTINCT t.transaction_no) as transaction_count,
    COUNT(DISTINCT t.mstr_customer_id) as unique_customers,
    SUM(t.amount) as total_amount,
    AVG(t.amount) as avg_transaction_amount,
    MIN(t.amount) as min_amount,
    MAX(t.amount) as max_amount,
    ROUND(SUM(CASE WHEN t.amount > 0 THEN t.amount ELSE 0 END), 2) as positive_amount,
    ROUND(SUM(CASE WHEN t.amount < 0 THEN t.amount ELSE 0 END), 2) as negative_amount
FROM
    sas_transaction_vw t
INNER JOIN
    sas_calendar cal ON t.transaction_date = cal.exact_day_dt
GROUP BY
    cal.fyr,
    cal.fmm,
    cal.cycq
ORDER BY
    cal.fyr DESC,
    cal.fmm DESC;
