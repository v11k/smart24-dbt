{% macro generate_ga4_union_traffic(company_name) %}
{% set property_ids = get_property_ids_for_company(company_name) %}
    {% do log('Property IDs for ' ~ company_name ~ ': ' ~ (property_ids | join(', ')), info=True) %}

    {% if property_ids | length == 0 %}
        {% do log('No property IDs found for company: ' ~ company_name, info=True) %}
    {% endif %}

    {% set union_queries = [] %}
    {% for property_id in property_ids %}
        {% set traffic_table_name = 's24_traffic_property' ~ property_id %}
        {% set properties_table = ref('properties_with_attribute') %}
        {% set query = 
        "SELECT \
        
        TO_DATE(tr.date, 'YYYYMMDD') as \"Date\", \
        p.account_display_name as \"Account name\", \
        p.account_id as \"Account ID\", \
        p.property_display_name as \"GA4 property\", \
        p.property_id as \"GA4 property ID\", \
        tr.\"sessionSourceMedium\" as \"Session source / medium\", \
        tr.\"sessionDefaultChannelGroup\" as \"Session default channel grouping\", \
        tr.\"sessionCampaignName\" as \"Session campaign name\", \
        tr.\"sessionManualTerm\" as \"Session manual term\", \
        tr.\"sessionManualAdContent\" as \"Session manual ad content\", \
        tr.\"activeUsers\" as \"Active users\", \
        tr.\"sessions\" as \"Sessions\", \
        tr.\"newUsers\" as \"New users\", \
        tr.\"engagedSessions\" as \"Engaged sessions\", \
        tr.\"screenPageViews\" as \"Views\", \
        tr.\"userEngagementDuration\" as \"Total user engagement duration (sec)\" \
        FROM ga4." ~ traffic_table_name ~ " tr \
        LEFT JOIN " ~ properties_table ~ " p ON p.property_id::text = tr.property_id::text \
        ORDER BY \"Date\" DESC"
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
