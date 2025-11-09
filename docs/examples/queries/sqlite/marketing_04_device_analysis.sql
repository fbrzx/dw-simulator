-- Device and User Agent Analysis
-- Analyzes email engagement by device type and user agent

SELECT
    eo.device_type,
    COUNT(DISTINCT eo.open_id) as total_opens,
    COUNT(DISTINCT eo.send_id) as unique_emails_opened,
    COUNT(DISTINCT ec.click_id) as total_clicks,
    ROUND(100.0 * COUNT(DISTINCT ec.click_id) / NULLIF(COUNT(DISTINCT eo.open_id), 0), 2) as device_ctr,
    COUNT(DISTINCT CASE WHEN eo.user_agent LIKE '%Mobile%' THEN eo.send_id END) as mobile_opens,
    COUNT(DISTINCT CASE WHEN eo.user_agent LIKE '%Chrome%' THEN eo.send_id END) as chrome_opens,
    COUNT(DISTINCT CASE WHEN eo.user_agent LIKE '%Safari%' THEN eo.send_id END) as safari_opens,
    COUNT(DISTINCT CASE WHEN eo.user_agent LIKE '%Firefox%' THEN eo.send_id END) as firefox_opens
FROM email_opens eo
LEFT JOIN email_clicks ec ON eo.send_id = ec.send_id
GROUP BY eo.device_type
ORDER BY total_opens DESC;
