{{ config(
    depends_on={'refs': ['properties_with_attribute']}
) }}

{% set company_name = get_company_name() %}  

{{ generate_ga4_union_ecom_roi(company_name)}}
