{{ config(
    depends_on={'refs': ['gads_accounts_with_attribute']}
) }}
{% set company_name = get_company_name() %}  

{{ get_gads_campaigns_query(company_name)}}