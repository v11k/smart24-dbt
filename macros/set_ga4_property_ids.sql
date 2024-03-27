{% macro get_property_ids_for_company(company_name) %}
    {% set query %}
        SELECT property_id
        FROM {{ ref('properties_with_attribute') }}
        WHERE attribute = '{{ company_name }}'
    {% endset %}

    {% set results = run_query(query) %}
    
    {% if execute %}
        {% set property_ids = results.columns[0].values() %}
    {% else %}
        {% set property_ids = [] %}
    {% endif %}

    {% if property_ids | length == 0 %}
        {% do log('No property IDs found for company: ' ~ company_name, info=True) %}
    {% endif %}

    {{ return(property_ids) }}
{% endmacro %}
