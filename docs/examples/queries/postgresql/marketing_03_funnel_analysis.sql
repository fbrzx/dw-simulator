-- Email Marketing Funnel Analysis
-- Tracks conversion from send → delivery → open → click
-- PostgreSQL/Redshift version: uses boolean comparison instead of = 1

SELECT
    'Total Sends' as stage,
    1 as stage_order,
    COUNT(DISTINCT es.send_id) as count,
    100.0 as percentage,
    0.0 as drop_off_rate
FROM email_sends es

UNION ALL

SELECT
    'Delivered' as stage,
    2 as stage_order,
    COUNT(DISTINCT CASE WHEN es.delivered THEN es.send_id END) as count,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN es.delivered THEN es.send_id END) / NULLIF(COUNT(DISTINCT es.send_id), 0), 2) as percentage,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN NOT es.delivered OR es.delivered IS NULL THEN es.send_id END) / NULLIF(COUNT(DISTINCT es.send_id), 0), 2) as drop_off_rate
FROM email_sends es

UNION ALL

SELECT
    'Opened' as stage,
    3 as stage_order,
    COUNT(DISTINCT eo.send_id) as count,
    ROUND(100.0 * COUNT(DISTINCT eo.send_id) / NULLIF((SELECT COUNT(DISTINCT send_id) FROM email_sends), 0), 2) as percentage,
    ROUND(100.0 * (COUNT(DISTINCT es.send_id) - COUNT(DISTINCT eo.send_id)) / NULLIF(COUNT(DISTINCT es.send_id), 0), 2) as drop_off_rate
FROM email_sends es
LEFT JOIN email_opens eo ON es.send_id = eo.send_id

UNION ALL

SELECT
    'Clicked' as stage,
    4 as stage_order,
    COUNT(DISTINCT ec.send_id) as count,
    ROUND(100.0 * COUNT(DISTINCT ec.send_id) / NULLIF((SELECT COUNT(DISTINCT send_id) FROM email_sends), 0), 2) as percentage,
    ROUND(100.0 * (COUNT(DISTINCT eo.send_id) - COUNT(DISTINCT ec.send_id)) / NULLIF(COUNT(DISTINCT eo.send_id), 0), 2) as drop_off_rate
FROM email_opens eo
LEFT JOIN email_clicks ec ON eo.send_id = ec.send_id

ORDER BY stage_order;
