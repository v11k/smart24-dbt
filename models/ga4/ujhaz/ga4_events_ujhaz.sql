{{ config(
    depends_on={'refs': ['properties_with_attribute']}
) }}

{% set company_name = get_company_name() %}  
{% set unioned_query = generate_ga4_union_events(company_name) %}
{{ unioned_query }}

