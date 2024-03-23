{% macro get_property_ids_for_company(company_name) %}
    {% set query %}
        SELECT property_id
        FROM {{ ref('properties_with_attribute') }}
        WHERE attribute = '{{ company_name }}'
    {% endset %}

    {% do log('Executing query for company_name: ' ~ company_name, info=True) %}
    {% do log('SQL Query: ' ~ query, info=True) %}

    {% set results = run_query(query) %}
    
    {% if execute %}
        {% set property_ids = results.columns[0].values() %}
        {% do log('Found property IDs: ' ~ property_ids | join(', '), info=True) %}
    {% else %}
        {% set property_ids = [] %}
        {% do log('Execution context not set, returning empty property IDs list.', info=True) %}
    {% endif %}

    {% if property_ids | length == 0 %}
        {% do log('No property IDs found for company: ' ~ company_name, info=True) %}
    {% endif %}

    {{ return(property_ids) }}
{% endmacro %}
