{% macro get_instagram_posts_query(company_name) %}

SELECT 
	cast(m.timestamp as date) as "Date",
	m.business_account_id as "User ID",
	m.username as "Name",
	CASE
            WHEN m.media_product_type::text = 'FEED'::text OR m.media_product_type IS NULL THEN m.media_type
            WHEN m.media_product_type::text = 'STORY'::text THEN concat(m.media_type, '_', m.media_product_type)::character varying
            WHEN m.media_product_type::text = 'REELS'::text THEN 'REEL'::character varying
            ELSE m.media_type
        END AS "Media type",
	m.media_url as "Media URL",
	m.permalink as "Media permalink",
	m.caption as "Media caption",
	coalesce(m.comments_count + m.like_count + i.saved,0) as "Engagement",
	m.comments_count as "Comments count",
	m.like_count as "Like count",
	coalesce(i.impressions,0) as "Media impressions",
	coalesce(i.reach,0) as "Media reach",
	coalesce(i.saved,0) as "Unique saves",
	m.id as "Post ID"
FROM {{ source('instagram', 'media') }} m
LEFT JOIN {{ source('instagram', 'media_insights') }} i
	on i.id = m.id
left join {{ ref("instagram_page_ids_with_attribute")}} a
	on a.id = m.business_account_id
WHERE 
   a.attribute = '{{ company_name }}'

{% endmacro %}