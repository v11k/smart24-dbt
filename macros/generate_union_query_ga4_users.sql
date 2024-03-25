{% macro generate_ga4_union_users(company_name) %}
{% set property_ids = get_property_ids_for_company(company_name) %}
    {% do log('Property IDs for ' ~ company_name ~ ': ' ~ (property_ids | join(', ')), info=True) %}

    {% if property_ids | length == 0 %}
        {% do log('No property IDs found for company: ' ~ company_name, info=True) %}
    {% endif %}

    {% set union_queries = [] %}
    {% for property_id in property_ids %}
        {% set users_table_name = 's24_pages_users' ~ property_id %}
        {% set properties_table = ref('properties_with_attribute') %}
        {% set query = "
        SELECT
            to_date(u.date, 'YYYYMMDD') as \"Date\",
            p.account_display_name as \"Account name\",
            p.account_id as \"Account ID\",
            p.property_display_name as \"GA4 property\",
            p.property_id as \"GA4 property ID\",
            u.\"firstUserSourceMedium\" as \"First user source / medium\",
            u.\"firstUserDefaultChannelGroup\" as \"First user default channel grouping\",
            u.\"firstUserCampaignName\" as \"User campaign name\",
            u.\"firstUserManualTerm\" as \"First user manual term\",
            u.\"firstUserManualAdContent\" as \"First user manual ad content\",
            u.\"activeUsers\" as \"Active users\",
            u.\"newUsers\" as \"New users\",
            u.sessions as \"Sessions\",
            u.\"screenPageViews\" as \"Views\",
            u.\"userEngagementDuration\" as \"Total user engagement (sec)\",
            u.\"engagedSessions\" as \"Engaged sessions\"
        FROM ga4." ~ users_table_name ~ " pa
        LEFT JOIN " ~ properties_table ~ " pr ON pr.property_id::text = pa.property_id::text
        "
        %}
        {% do union_queries.append(query) %}
        {% do log('Generated query for property ID ' ~ property_id, info=True) %}
    {% endfor %}
    {% if union_queries | length == 0 %}
        {% do log('No queries generated for company: ' ~ company_name, info=True) %}
    {% else %}
        {{ union_queries | join(' UNION ALL ') }}
    {% endif %}
{% endmacro %}
