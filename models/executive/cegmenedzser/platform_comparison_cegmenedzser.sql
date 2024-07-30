{{ config(
    depends_on={'refs': [
                        'fbads_ads_' ~ get_company_name(), 
                        'gads_ads_' ~ get_company_name(),
                        'fbpages_posts_' ~ get_company_name(),
                        'insta_posts_' ~ get_company_name(),
                        'ga4_traffic_' ~ get_company_name(),
                        'ga4_conversions_' ~ get_company_name(),
                        ]}
) }}
{% set company_name = get_company_name() %}


{{ get_executive_platform_comparison_query(company_name) }}