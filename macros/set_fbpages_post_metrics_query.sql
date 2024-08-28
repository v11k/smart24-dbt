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
    FROM reaction_values
),
aggregated_reactions as(
	select 
		post_id,
		sum(case when reaction_type = 'like' then expanded_reactions.reaction_count else 0 end) as post_reactions_like,
    	sum(case when reaction_type = 'love' then expanded_reactions.reaction_count else 0 end) as post_reactions_love,
    	sum(case when reaction_type = 'haha' then expanded_reactions.reaction_count else 0 end) as post_reactions_haha,
    	sum(case when reaction_type = 'wow' then expanded_reactions.reaction_count else 0 end) as post_reactions_wow,
    	sum(case when reaction_type = 'anger' then expanded_reactions.reaction_count else 0 end) as post_reactions_anger,
    	sum(case when reaction_type = 'sorry' then expanded_reactions.reaction_count else 0 end) as post_reactions_sorry,
		sum(coalesce(reaction_count,0)) as post_reactions_total
	from expanded_reactions
	group by post_id
),
insights_with_post_id AS (
    SELECT 
        substring(id FROM 1 FOR position('/' IN id) - 1) as post_id,
		sum(case when name = 'post_impressions_unique' then (values -> 0 ->> 'value')::numeric else 0 end) as post_reach,
    	sum(case when name = 'post_impressions' then (values -> 0 ->> 'value')::numeric else 0 end) as post_impressions,
		sum(case when name = 'post_engaged_users' then (values -> 0 ->> 'value')::numeric else 0 end) as post_engaged_users
    FROM 
        facebook_pages_custom.{{ company_name }}_post_insights
	GROUP BY 1
)
SELECT 
    i.post_id,
    i.post_reach,
    i.post_impressions,
	i.post_engaged_users,
    ar.post_reactions_like,
    ar.post_reactions_love,
    ar.post_reactions_haha,
    ar.post_reactions_wow,
    ar.post_reactions_anger,
    ar.post_reactions_sorry,
	ar.post_reactions_total
FROM 
    insights_with_post_id i
LEFT JOIN
    aggregated_reactions ar ON i.post_id = ar.post_id


{% endmacro %}