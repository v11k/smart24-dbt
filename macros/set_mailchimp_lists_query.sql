{% macro get_mailchimp_lists_query(schema_name) %}

SELECT
	name as "List name",
	list_rating as "List rating",
	date_created as "List created",
	visibility as "List visibility",
	(stats->>'campaign_count')::numeric as "Campaign count",
	(stats->>'member_count')::numeric as "Members",
	(stats->>'unsubscribe_count')::numeric as "Unsubscribes"
FROM
{{ schema_name }}.lists

{% endmacro %}