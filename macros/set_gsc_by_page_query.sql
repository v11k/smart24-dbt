{% macro get_gsc_by_page_query(company_name) %}

SELECT 
    s.site_url,
    s.date,
    s.search_type,
    s.page,
    s.position,
    s.impressions,
    s.clicks
from {{ source('gsc', 'search_analytics_by_page') }} s
left join {{ ref("gsc_sites_with_attribute")}} a
    on a.site_url = s.site_url
WHERE 
   a.attribute = '{{ company_name }}'

{% endmacro %}