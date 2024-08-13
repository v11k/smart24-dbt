{{ config(
    depends_on={'refs': ['fbpages_posts_divatekszer1', 'fbpages_posts_divatekszer2', 'fbpages_posts_divatekszer3', 'fbpages_posts_divatekszer4']}
) }} 

select * from {{ ref("fbpages_posts_divatekszer1")}}
UNION
select * from {{ ref("fbpages_posts_divatekszer2")}}
UNION
select * from {{ ref("fbpages_posts_divatekszer3")}}
UNION
select * from {{ ref("fbpages_posts_divatekszer4")}}