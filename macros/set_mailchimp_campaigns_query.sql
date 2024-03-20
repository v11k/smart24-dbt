{% macro get_mailchimp_campaigns_query(schema_name) %}

SELECT
    c.send_time as "Date",
    c.settings->>'title' AS "Campaign name",
    c.recipients->>'list_name' as "List name",
    c.type as "Campaign type",
    c.status as "Status",
    c.create_time as "Created",
    c.emails_sent as "Emails sent",
    c.emails_sent - ((r.bounces->>'hard_bounces')::numeric + (r.bounces->>'soft_bounces')::numeric) as "Emails delivered",
    (c.report_summary->>'unique_opens')::numeric as "Unique opens",
    r.unsubscribed as "Unsubscribes",
    (r.forwards->>'forwards_count')::numeric as "Forwards",
    (r.clicks->>'unique_clicks')::numeric as "Unique clicks",
    (r.bounces->>'hard_bounces')::numeric + (r.bounces->>'soft_bounces')::numeric as "Bounces"
FROM {{ schema_name }}.campaigns c
LEFT JOIN {{ schema_name }}.reports r ON r.id = c.id
WHERE c.emails_sent > 0
ORDER BY "Date"

{% endmacro %}