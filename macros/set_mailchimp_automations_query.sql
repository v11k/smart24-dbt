{% macro get_mailchimp_automations_query(schema_name) %}

SELECT
	a.status as "automation_status",
	coalesce((a.report_summary->>'unique_opens')::numeric,0) as "unique_opens",
	a.emails_sent,
	a.trigger_settings->>'workflow_type' as "automation_type",
	a.create_time as "created",
	a.id as "automation_id",
    round(coalesce((a.report_summary->>'unique_opens')::numeric / nullif((a.report_summary->>'open_rate')::numeric,0),0)) as "emails_delivered",
	coalesce((a.report_summary->>'subscriber_clicks')::numeric,0) as "unique_clicks",
	a.settings->>'title' as "automation_name"
FROM
{{ schema_name }}.automations a

{% endmacro %}