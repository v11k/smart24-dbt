{% macro get_fbpages_posts_query(company_name) %}

select
	po.created_time as "Post creation date",
	pa.name as "Page name",
	pa.id as "Page ID",
	po.id as "Post ID",
	po.message as "Post message",
	po.permalink_url as "Link to post",
	po.icon as "Post type",
	case when po.status_type like '%share%' or po.status_type like '%mobile_status_update%' then 'share'
		when po.status_type like '%photo%' then 'photo'
		when po.status_type like '%video%' then 'video'
		else replace(po.status_type, '_', ' ') end as "Post status type",
	po.picture as "Post image URL",
	coalesce((po.shares ->> 'count')::numeric,0) as "Shares on posts",
	coalesce(jsonb_array_length(comments -> 'data'),0) as "Comments on posts",
	m.post_reach as "Post reach",
	m.post_impressions as "Post impressions",
	m.post_engaged_users as "Post engaged users",
	m.post_reactions_like as "Post reactions: like",
	m.post_reactions_wow as "Post reactions: wow",
	m.post_reactions_haha as "Post reactions: haha",
	m.post_reactions_anger as "Post reactions: angry",
	m.post_reactions_love as "Post reactions: love",
	m.post_reactions_sorry as "Post reactions: sad",
	m.post_reactions_like + m.post_reactions_wow + m.post_reactions_haha + m.post_reactions_anger + m.post_reactions_love + m.post_reactions_sorry as "Total post reactions"
from {{ ref('facebook_pages_custom.' ~ company_name ~ '_post') }} po
left join {{ ref('facebook_pages_custom.' ~ company_name ~ '_page') }} pa
	on split_part(po.id, '_', 1) = pa.id
left join {{ ref("fbpages_post_metrics_" ~ company_name)}} m
	on m.post_id = po.id

{% endmacro %}