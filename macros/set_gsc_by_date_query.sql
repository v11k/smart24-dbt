{% macro get_gsc_by_date_query(company_name) %}

SELECT 
    s.site_url,
    s.date,
    s.search_type,
    s.position,
    s.impressions,
    s.clicks
from {{ source('gsc', 'search_analytics_by_date') }} s
left join {{ ref("gsc_sites_with_attribute")}} a
    on a.site_url = s.site_url
WHERE 
   a.attribute = '{{ company_name }}'

{% endmacro %}