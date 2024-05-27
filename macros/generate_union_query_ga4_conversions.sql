{% macro generate_ga4_union_conversions(company_name) %}
{% set property_ids = get_property_ids_for_company(company_name) %}
    {% do log('Property IDs for ' ~ company_name ~ ': ' ~ (property_ids | join(', ')), info=True) %}

    {% if property_ids | length == 0 %}
        {% do log('No property IDs found for company: ' ~ company_name, info=True) %}
    {% endif %}

    {% set union_queries = [] %}
    {% for property_id in property_ids %}
        {% set conversions_table_name = 's24_conversions_property' ~ property_id %}
        {% set properties_table = ref('properties_with_attribute') %}
        {% set query = "
        SELECT
            to_date(c.date, 'YYYYMMDD') as \"Date\",
            pr.account_display_name as \"Account name\",
            pr.account_id as \"Account ID\",
            pr.property_display_name as \"GA4 property\",
            pr.property_id as \"GA4 property ID\",
            c.\"eventName\" as \"Event name\",
            c.\"defaultChannelGroup\" as \"Conversions default channel grouping\",
            c.\"sourceMedium\" as \"Source / medium\",
            c.\"campaignName\" as \"Event campaign name\",
            c.\"manualTerm\" as \"Manual term\",
            c.\"manualAdContent\" as \"Manual ad content\",
            c.\"keyEvents\" as \"Conversions\",
            c.\"eventValue\" as \"Event value\",
            c.\"totalUsers\" as \"Total users\"
        FROM ga4." ~ conversions_table_name ~ " c
        LEFT JOIN " ~ properties_table ~ " pr ON pr.property_id::text = c.property_id::text
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
