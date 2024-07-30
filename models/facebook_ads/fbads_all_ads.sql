{{ config(
    depends_on={'refs': ['fbads_accounts_with_attribute']}
) }}
{% set company_name = '%' %}  

{{ get_fbads_ads_query(company_name)}}