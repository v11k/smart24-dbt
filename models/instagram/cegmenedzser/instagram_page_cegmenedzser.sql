{{ config(
    depends_on={'refs': ['get_instagram_page_ids_for_company']}
) }}

{% set company_name = get_company_name() %}  
{{ get_instagram_page_query(company_name) }}

