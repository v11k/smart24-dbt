WITH gads_accounts as (
    select distinct customer_id, customer_descriptive_name from google_ads.customer
),
accounts as (
    select id, name, account, platform, attribute, account_level_2 
    from google_drive.accounts
    where platform = 'gads'
)

SELECT 
    gads_accounts.customer_id,
    gads_accounts.customer_descriptive_name,
    accounts.attribute
from gads_accounts
left join accounts on accounts.account::text = gads_accounts.customer_id::text

