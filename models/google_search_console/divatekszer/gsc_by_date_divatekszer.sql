{{ config(
    depends_on={'refs': ['gsc_sites_with_attribute']}
) }}
{% set company_name = get_company_name() %}  

{{ get_gsc_by_date_query(company_name)}}