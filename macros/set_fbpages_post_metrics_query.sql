{% macro get_fbpages_post_metrics_query(company_name) %}

WITH reaction_values AS (
    SELECT
        substring(id FROM 1 FOR position('/' IN id) - 1) as post_id,
        name,
        (values -> 0 -> 'value') AS json_object
    FROM 
        facebook_pages_custom.{{ company_name }}_post_insights
    WHERE 
        name = 'post_reactions_by_type_total'
),
expanded_reactions AS (
    SELECT
        post_id,
        (jsonb_each_text(json_object)).key AS reaction_type,
        (jsonb_each_text(json_object)).value::int AS reaction_count
    FROM 
        reaction_values
),
insights_with_post_id AS (
    SELECT 
        substring(id FROM 1 FOR position('/' IN id) - 1) as post_id,
        id,
        name,
        values
    FROM 
        facebook_pages_custom.{{ company_name }}_post_insights
)
SELECT 
    insights_with_post_id.post_id,
    sum(case when insights_with_post_id.name = 'post_impressions_unique' then (insights_with_post_id.values -> 0 ->> 'value')::numeric else 0 end) as post_reach,
    sum(case when insights_with_post_id.name = 'post_impressions' then (insights_with_post_id.values -> 0 ->> 'value')::numeric else 0 end) as post_impressions,
	sum(case when insights_with_post_id.name = 'post_engaged_users' then (insights_with_post_id.values -> 0 ->> 'value')::numeric else 0 end) as post_engaged_users,
    sum(case when expanded_reactions.reaction_type = 'like' then expanded_reactions.reaction_count else 0 end) as post_reactions_like,
    sum(case when expanded_reactions.reaction_type = 'love' then expanded_reactions.reaction_count else 0 end) as post_reactions_love,
    sum(case when expanded_reactions.reaction_type = 'haha' then expanded_reactions.reaction_count else 0 end) as post_reactions_haha,
    sum(case when expanded_reactions.reaction_type = 'wow' then expanded_reactions.reaction_count else 0 end) as post_reactions_wow,
    sum(case when expanded_reactions.reaction_type = 'anger' then expanded_reactions.reaction_count else 0 end) as post_reactions_anger,
    sum(case when expanded_reactions.reaction_type = 'sorry' then expanded_reactions.reaction_count else 0 end) as post_reactions_sorry,
	sum(coalesce(expanded_reactions.reaction_count,0)) as post_reactions_total
FROM 
    insights_with_post_id
LEFT JOIN
    expanded_reactions ON insights_with_post_id.post_id = expanded_reactions.post_id
GROUP BY 
    insights_with_post_id.post_id;

{% endmacro %}