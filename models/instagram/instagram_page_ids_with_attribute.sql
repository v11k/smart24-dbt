WITH pages as (
    select * from "airbyte"."instagram"."users" 
),
accounts as (
    select id, name, account, platform, attribute, account_level_2 
    from {{ source('google_drive_clients', 'client_accounts') }} 
    where platform = 'igi'
)

SELECT 
    pages.page_id,
    pages.name,
    pages.username,
	pages.id,
    accounts.attribute
from pages
left join accounts on accounts.account::text = pages.id::text

