{% macro get_fbpages_page_impressions_query(company_name) %}

select 
	split_part(i.id, '/', 1) as page_id,
	p.name as page_name,
	case when i.name = 'page_impressions_paid' then 'Fizetett'
		when i.name = 'page_impressions_organic_v2' then 'Organikus'
		else null end as impression_type,
	(i.values -> 1 -> 'value')::int as page_impressions_28

from facebook_pages_custom.{{ company_name }}_page_insights i
left join facebook_pages_custom.{{ company_name }}_page p
	on p.id = split_part(i.id, '/', 1)
where (i.name = 'page_impressions_organic_v2'
	or i.name = 'page_impressions_paid')
	and i.period = 'days_28'


{% endmacro %}