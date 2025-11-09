-- Campaign Performance Overview
-- Tracks key metrics for each marketing campaign: sends, opens, clicks, and conversion rates

SELECT
    c.campaign_id,
    c.campaign_name,
    c.campaign_type,
    c.start_date,
    c.end_date,
    c.budget,
    c.target_audience,
    COUNT(DISTINCT es.send_id) as total_sends,
    COUNT(DISTINCT es.subscriber_id) as unique_recipients,
    COUNT(DISTINCT CASE WHEN es.delivered = 1 THEN es.send_id END) as delivered_emails,
    COUNT(DISTINCT eo.open_id) as total_opens,
    COUNT(DISTINCT ec.click_id) as total_clicks,
    ROUND(100.0 * COUNT(DISTINCT eo.open_id) / NULLIF(COUNT(DISTINCT es.send_id), 0), 2) as open_rate,
    ROUND(100.0 * COUNT(DISTINCT ec.click_id) / NULLIF(COUNT(DISTINCT eo.open_id), 0), 2) as click_through_rate,
    ROUND(100.0 * COUNT(DISTINCT ec.click_id) / NULLIF(COUNT(DISTINCT es.send_id), 0), 2) as click_to_send_rate
FROM campaigns c
LEFT JOIN email_sends es ON c.campaign_id = es.campaign_id
LEFT JOIN email_opens eo ON es.send_id = eo.send_id
LEFT JOIN email_clicks ec ON es.send_id = ec.send_id
GROUP BY
    c.campaign_id,
    c.campaign_name,
    c.campaign_type,
    c.start_date,
    c.end_date,
    c.budget,
    c.target_audience
HAVING total_sends > 0
ORDER BY total_sends DESC;
