{% macro generate_ga4_union_pages(company_name) %}
{% set property_ids = get_property_ids_for_company(company_name) %}
    {% do log('Property IDs for ' ~ company_name ~ ': ' ~ (property_ids | join(', ')), info=True) %}

    {% if property_ids | length == 0 %}
        {% do log('No property IDs found for company: ' ~ company_name, info=True) %}
    {% endif %}

    {% set union_queries = [] %}
    {% for property_id in property_ids %}
        {% set pages_table_name = 's24_pages_property' ~ property_id %}
        {% set properties_table = ref('properties_with_attribute') %}
        {% set query = "
        SELECT
        TO_DATE(pa.date, 'YYYYMMDD') AS \"Date\",
        pr.account_display_name AS \"Account name\",
        pr.account_id AS \"Account ID\",
        pr.property_display_name AS \"GA4 property\",
        pr.property_id AS \"GA4 property ID\",
        pa.\"hostName\" AS \"Host name\",
        pa.\"pageTitle\" AS \"Page title\",
        pa.\"pagePath\" AS \"Page path without query string\",
        pa.\"activeUsers\" AS \"Active users\",
        pa.\"screenPageViews\" AS \"Views\",
        pa.\"userEngagementDuration\" AS \"Total user engagement duration (sec)\",
        pa.sessions AS \"Sessions\",
        pa.\"engagedSessions\" AS \"Engaged sessions\"
        FROM ga4." ~ pages_table_name ~ " pa
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
