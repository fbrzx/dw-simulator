-- Daily Email Performance Time Series
-- Tracks daily trends in email sends, opens, and clicks

SELECT
    es.send_date,
    COUNT(DISTINCT es.send_id) as sends,
    COUNT(DISTINCT CASE WHEN es.delivered = 1 THEN es.send_id END) as delivered,
    COUNT(DISTINCT eo.open_id) as opens,
    COUNT(DISTINCT ec.click_id) as clicks,
    COUNT(DISTINCT es.campaign_id) as active_campaigns,
    ROUND(100.0 * COUNT(DISTINCT eo.open_id) / NULLIF(COUNT(DISTINCT es.send_id), 0), 2) as daily_open_rate,
    ROUND(100.0 * COUNT(DISTINCT ec.click_id) / NULLIF(COUNT(DISTINCT eo.open_id), 0), 2) as daily_ctr
FROM email_sends es
LEFT JOIN email_opens eo ON es.send_id = eo.send_id AND es.send_date = eo.open_date
LEFT JOIN email_clicks ec ON es.send_id = ec.send_id AND es.send_date = ec.click_date
GROUP BY es.send_date
ORDER BY es.send_date;
