WITH fbads_accounts as (
    select distinct account_id, account_name from {{ source('facebook_ads', 'ads_insights') }} 
),
accounts as (
    select id, name, account, platform, attribute, account_level_2 
    from {{ source('google_drive_clients', 'client_accounts') }} 
    where platform = 'fbads'
)

SELECT 
    fb.account_id, 
	fb.account_name,
    a.attribute
from fbads_accounts fb
left join accounts a on a.account::text = fb.account_id::text