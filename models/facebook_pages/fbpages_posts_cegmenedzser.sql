{% set company_name = get_company_name() %}

{{ config(
    depends_on={'refs': ['fbpages_post_metrics_' ~ company_name]}
) }}

{{ get_fbpages_posts_query(company_name) }}
