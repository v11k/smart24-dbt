{{ config(
    depends_on={'refs': ['fbpages_page_impressions_divatekszer1', 'fbpages_page_impressions_divatekszer2', 'fbpages_page_impressions_divatekszer3', 'fbpages_page_impressions_divatekszer4']}
) }} 

select * from {{ ref("fbpages_page_impressions_divatekszer1")}}
UNION
select * from {{ ref("fbpages_page_impressions_divatekszer2")}}
UNION
select * from {{ ref("fbpages_page_impressions_divatekszer3")}}
UNION
select * from {{ ref("fbpages_page_impressions_divatekszer4")}}