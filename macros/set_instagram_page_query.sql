{% macro get_instagram_page_query(company_name) %}

 SELECT cast(ui.date as date) AS "Date",
    u.page_id::text AS "User ID",
    u.name AS "Full name",
    u.username AS "Name",
    u.id as "Account ID",
    ui.profile_views AS "Profile views",
    ui.website_clicks AS "Website clicks",
    ui.impressions AS "Profile impressions",
    ui.reach AS "Profile reach",
    coalesce(ui.follower_count,0) AS "Profile followers"
from instagram.users u
left join instagram.user_insights ui
    on u.page_id = ui.page_id
left join {{ ref("instagram_page_ids_with_attribute")}} a
    on a.id = u.id
WHERE 
   attribute = '{{ company_name }}'

{% endmacro %}