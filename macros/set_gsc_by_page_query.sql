{% macro get_gsc_by_page_query(company_name) %}

SELECT 
    s.site_url,
    s.date,
    s.search_type,
    CASE 
        WHEN s.page ~* '^https?://[^/]+/?$' THEN s.page
        ELSE REGEXP_REPLACE(s.page, '^(https?://[^/]+/?)', '')
    END AS page,
    s.position AS current_position,
    s.impressions AS current_impressions,
    s.clicks AS current_clicks,
    s_prev.position AS position_30_days_ago,
    s_prev.impressions AS impressions_30_days_ago,
    s_prev.clicks AS clicks_30_days_ago
FROM 
    {{ source('google_search_console', 'search_analytics_by_page') }} s
LEFT JOIN 
    {{ ref("gsc_sites_with_attribute") }} a
    ON a.site_url = s.site_url
LEFT JOIN 
    {{ source('google_search_console', 'search_analytics_by_page') }} s_prev
    ON s.site_url = s_prev.site_url
    AND s.search_type = s_prev.search_type
    AND s.page = s_prev.page
    AND s.date = s_prev.date + INTERVAL '30 days'
WHERE 
    a.attribute = '{{ company_name }}'
{% endmacro %}