{{ config(
    depends_on={'refs': ['fbpages_post_metrics_divatekszer1', 'fbpages_post_metrics_divatekszer2', 'fbpages_post_metrics_divatekszer3', 'fbpages_post_metrics_divatekszer4']}
) }} 

select * from {{ ref("fbpages_post_metrics_divatekszer1")}}
UNION
select * from {{ ref("fbpages_post_metrics_divatekszer2")}}
UNION
select * from {{ ref("fbpages_post_metrics_divatekszer3")}}
UNION
select * from {{ ref("fbpages_post_metrics_divatekszer4")}}