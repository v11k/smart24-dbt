{{ config(
    depends_on={'refs': ['fbpages_post_metrics_' ~ get_company_name(),'fbads_boosted_posts']}
) }}
{% set company_name = get_company_name() %}


{{ get_fbpages_posts_query(company_name) }}
