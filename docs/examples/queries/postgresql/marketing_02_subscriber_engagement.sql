-- Subscriber Engagement Analysis
-- Analyzes individual subscriber behavior and engagement levels
-- PostgreSQL/Redshift version: uses boolean comparison instead of = 1

SELECT
    s.subscriber_id,
    s.email,
    s.first_name,
    s.last_name,
    s.company,
    s.job_title,
    s.subscription_date,
    COUNT(DISTINCT es.send_id) as emails_received,
    COUNT(DISTINCT eo.open_id) as emails_opened,
    COUNT(DISTINCT ec.click_id) as emails_clicked,
    ROUND(100.0 * COUNT(DISTINCT eo.open_id) / NULLIF(COUNT(DISTINCT es.send_id), 0), 2) as personal_open_rate,
    ROUND(100.0 * COUNT(DISTINCT ec.click_id) / NULLIF(COUNT(DISTINCT eo.open_id), 0), 2) as personal_ctr,
    MAX(eo.open_date) as last_open_date,
    MAX(ec.click_date) as last_click_date,
    CASE
        WHEN COUNT(DISTINCT ec.click_id) > 5 THEN 'Highly Engaged'
        WHEN COUNT(DISTINCT eo.open_id) > 10 THEN 'Engaged'
        WHEN COUNT(DISTINCT eo.open_id) > 0 THEN 'Low Engagement'
        ELSE 'No Engagement'
    END as engagement_level
FROM marketing_campaigns__subscribers s
LEFT JOIN marketing_campaigns__email_sends es ON s.subscriber_id = es.subscriber_id
LEFT JOIN marketing_campaigns__email_opens eo ON es.send_id = eo.send_id
LEFT JOIN marketing_campaigns__email_clicks ec ON es.send_id = ec.send_id
WHERE s.is_subscribed = true
GROUP BY
    s.subscriber_id,
    s.email,
    s.first_name,
    s.last_name,
    s.company,
    s.job_title,
    s.subscription_date
HAVING COUNT(DISTINCT es.send_id) > 0
ORDER BY emails_clicked DESC, emails_opened DESC
LIMIT 100;
