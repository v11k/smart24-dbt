{% macro generate_ga4_union_demographics(company_name) %}
{% set property_ids = get_property_ids_for_company(company_name) %}
    {% do log('Property IDs for ' ~ company_name ~ ': ' ~ (property_ids | join(', ')), info=True) %}

    {% if property_ids | length == 0 %}
        {% do log('No property IDs found for company: ' ~ company_name, info=True) %}
    {% endif %}

    {% set union_queries = [] %}
    {% for property_id in property_ids %}
        {% set demographics_table_name = 's24_demographics_property' ~ property_id %}
        {% set properties_table = ref('properties_with_attribute') %}
        {% set query = "
        SELECT
            to_date(d.date, 'YYYYMMDD') as \"Date\",
            pr.account_display_name as \"Account name\",
            pr.account_id as \"Account ID\",
            pr.property_display_name as \"GA4 property\",
            pr.property_id as \"GA4 property ID\",
            d.\"deviceCategory\" as \"Device category\",
            d.language as \"Language\",
            d.country as \"Country\",
            d.region as \"Region\",
            d.city as \"City\",
            d.\"activeUsers\" as \"Active users\",
            d.\"newUsers\" as \"New users\",
            d.sessions as \"Sessions\",
            d.\"screenPageViews\" as \"Views\",
            d.\"userEngagementDuration\" as \"Total user engagement (sec)\",
            d.\"engagedSessions\" as \"Engaged sessions\"
        FROM ga4." ~ demographics_table_name ~ " d
        LEFT JOIN " ~ properties_table ~ " pr ON pr.property_id::text = d.property_id::text
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
