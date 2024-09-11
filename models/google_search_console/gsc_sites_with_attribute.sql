WITH sites as (
    select "siteUrl" as site_url from {{ source('google_search_console', 'sites') }} 
),
accounts as (
    select id, name, account, platform, attribute, account_level_2 
    from {{ source('google_drive_clients', 'client_accounts') }} 
    where platform = 'gsc'
)

SELECT 
    s.site_url,
    a.attribute
from sites s
left join accounts a on a.account::text = s.site_url::text