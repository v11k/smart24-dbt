{{ config(
    depends_on={'refs': ['fbpages_likes_divatekszer1', 'fbpages_likes_divatekszer2', 'fbpages_likes_divatekszer3', 'fbpages_likes_divatekszer4']}
) }} 

select * from {{ ref("fbpages_likes_divatekszer1")}}
UNION
select * from {{ ref("fbpages_likes_divatekszer2")}}
UNION
select * from {{ ref("fbpages_likes_divatekszer3")}}
UNION
select * from {{ ref("fbpages_likes_divatekszer4")}}