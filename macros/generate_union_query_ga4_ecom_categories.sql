{% macro generate_ga4_union_ecom_categories(company_name) %}
{% set property_ids = get_property_ids_for_company(company_name) %}
    {% do log('Property IDs for ' ~ company_name ~ ': ' ~ (property_ids | join(', ')), info=True) %}

    {% if property_ids | length == 0 %}
        {% do log('No property IDs found for company: ' ~ company_name, info=True) %}
    {% endif %}

    {% set union_queries = [] %}
    {% for property_id in property_ids %}
        {% set ecom_items_table_name = 's24_ecom_items_property' ~ property_id %}
        {% set properties_table = ref('properties_with_attribute') %}
        {% set query = "
        SELECT
            to_date(e.date, 'YYYYMMDD') as \"Date\",
            pr.account_display_name as \"Account name\",
            pr.account_id as \"Account ID\",
            pr.property_display_name as \"GA4 property\",
            pr.property_id as \"GA4 property ID\",
            e.\"itemCategory\" as \"Item category\",
            e.\"itemCategory2\" as \"Item category 2\",
            e.\"itemBrand\" as \"Item brand\",
            sum(e.\"itemsViewed\") as \"Items viewed\",
            sum(e.\"itemsPurchased\") as \"Items purchased\",
            sum(e.\"itemsCheckedOut\") as \"Items checked out\",
            sum(e.\"itemsAddedToCart\") as \"Items added to cart\",
            sum(e.\"itemRevenue\") as \"Item revenue\"
        FROM ga4." ~ ecom_items_table_name ~ " e
        LEFT JOIN " ~ properties_table ~ " pr ON pr.property_id::text = e.property_id::text
        WHERE (e.\"itemsViewed\" + e.\"itemsAddedToCart\" + e.\"itemsCheckedOut\" + e.\"itemsPurchased\" + e.\"itemRevenue\") <> 0
        GROUP BY 1,2,3,4,5,6,7,8
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
