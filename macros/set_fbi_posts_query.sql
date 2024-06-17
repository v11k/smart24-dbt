{% macro get_fbi_posts_query(company_name) %}

 SELECT DISTINCT "FBI_posts".post_creation_date AS "Post creation date",
    "FBI_posts".page_name AS "Page name",
    "FBI_posts".page_app_id::text AS "Page ID",
    "FBI_posts".post_id AS "Post ID",
    "FBI_posts".post_message AS "Post message",
    "FBI_posts".post_description AS "Post description",
    "FBI_posts".link_to_post AS "Link to post",
    "FBI_posts".post_type AS "Post type",
    "FBI_posts".post_status_type AS "Post status type",
    "FBI_posts".post_image_url AS "Post image URL",
    COALESCE(cast("FBI_posts".post_reach as numeric), 0) AS "Post reach",
    COALESCE(cast("FBI_posts".post_impressions as numeric), 0) AS "Post impressions",
    COALESCE(cast("FBI_posts".total_post_reactions as numeric), 0) AS "Total post reactions",
    COALESCE(cast("FBI_posts".post_comments as numeric), 0) AS "Comments on posts",
    COALESCE(cast("FBI_posts".post_shares as numeric), 0) AS "Shares on posts",
    COALESCE(cast("FBI_posts".post_likes as numeric), 0) AS "Likes on posts",
    COALESCE(cast("FBI_posts".post_engaged_users as numeric), 0) AS "Post engaged users",
    COALESCE(cast("FBI_posts".post_likes as numeric), 0) AS "Post likes",
    COALESCE(cast("FBI_posts".post_comments as numeric), 0) AS "Post comments",
    COALESCE(cast("FBI_posts".post_likes as numeric), 0) AS "Post reactions: like",
    COALESCE(cast("FBI_posts".post_reactions_wow as numeric), 0) AS "Post reactions: wow",
    COALESCE(cast("FBI_posts".post_reactions_love as numeric), 0) AS "Post reactions: love",
    COALESCE(cast("FBI_posts".post_reactions_haha as numeric), 0) AS "Post reactions: haha",
    COALESCE(cast("FBI_posts".post_reactions_sad as numeric), 0) AS "Post reactions: sad",
    COALESCE(cast("FBI_posts".post_reactions_angry as numeric), 0) AS "Post reactions: angry",
    CASE
      WHEN "FBAds"."Promoted post ID" IS NULL THEN 'Nem boost'::text
            ELSE 'Boost'::text
    END AS boost
		
FROM {{ source('google_drive_fbi', 'fbi_postsview') }}  "FBI_posts"
LEFT JOIN ( SELECT DISTINCT "Promoted post ID"
	FROM {{ source('facebook_ads', 'fbads') }} ) "FBAds" ON "FBAds"."Promoted post ID"::text = "FBI_posts".post_id::text
WHERE "FBI_posts".page_app_id::text IN 
    (select account::TEXT 
        as account from {{ source('google_drive_clients', 'client_accounts') }}
    where platform = 'fbi' and attribute = '{{ company_name }}')  
		

{% endmacro %}