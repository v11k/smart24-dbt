{{ config(
    depends_on={'refs': ['instagram_page_ids_with_attribute']}
) }}
{% set company_name = get_company_name() %}  

{{ get_instagram_followers_query(company_name)}}