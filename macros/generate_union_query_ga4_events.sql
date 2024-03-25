{% macro generate_ga4_union_events(company_name) %}
{% set property_ids = get_property_ids_for_company(company_name) %}
    {% do log('Property IDs for ' ~ company_name ~ ': ' ~ (property_ids | join(', ')), info=True) %}

    {% if property_ids | length == 0 %}
        {% do log('No property IDs found for company: ' ~ company_name, info=True) %}
    {% endif %}

    {% set union_queries = [] %}
    {% for property_id in property_ids %}
        {% set events_table_name = 's24_pages_events' ~ property_id %}
        {% set properties_table = ref('properties_with_attribute') %}
        {% set query = "
        SELECT
            to_date(e.date, 'YYYYMMDD') as \"Date\",
            p.account_display_name as \"Account name\",
            p.account_id as \"Account ID\",
            p.property_display_name as \"GA4 property\",
            p.property_id as \"GA4 property ID\",
            e.\"eventName\" as \"Event name\",
            e.\"totalUsers\" as \"Total users\"
        FROM ga4." ~ events_table_name ~ " pa
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
