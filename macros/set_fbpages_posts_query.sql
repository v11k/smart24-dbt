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
	coalesce(m.post_reach,0) as "Post reach",
	coalesce(m.post_impressions,0) as "Post impressions",
	coalesce(m.post_engaged_users,0) as "Post engaged users",
	coalesce(m.post_reactions_like,0) as "Post reactions: like",
	coalesce(m.post_reactions_wow,0) as "Post reactions: wow",
	coalesce(m.post_reactions_haha,0) as "Post reactions: haha",
	coalesce(m.post_reactions_anger,0) as "Post reactions: angry",
	coalesce(m.post_reactions_love,0) as "Post reactions: love",
	coalesce(m.post_reactions_sorry,0) as "Post reactions: sad",
	coalesce(m.post_reactions_like + m.post_reactions_wow + m.post_reactions_haha + m.post_reactions_anger + m.post_reactions_love + m.post_reactions_sorry,0) as "Total post reactions",
	case when b.fbpages_id is null then 'Nem boost' else 'Boost' end as boost
from facebook_pages_custom.{{ company_name }}_post po
left join facebook_pages_custom.{{ company_name }}_page pa
	on split_part(po.id, '_', 1) = pa.id
left join {{ ref("fbpages_post_metrics_" ~ company_name)}} m
	on m.post_id = po.id
left join {{ ref("fbads_boosted_posts")}} b
	on b.fbpages_id = po.id
where po.is_eligible_for_promotion <> 'false'
{% endmacro %}