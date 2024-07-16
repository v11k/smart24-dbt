{% macro get_fbpages_likes_query(company_name) %}

with new_likes as(
select 
	split_part(id, '/', 1) as page_id,
	(values -> 1 -> 'value' ->> 'total')::int as new_likes_28
from facebook_pages_custom.{{ company_name }}_page_insights
where name = 'page_fan_adds_by_paid_non_paid_unique'
	and period = 'days_28'
)
select 
	p.id,
	p.name,
	fan_count,
	n.new_likes_28
from facebook_pages_custom.{{ company_name }}_page p
left join new_likes n
	on p.id = n.page_id


{% endmacro %}