WITH properties as (
    select * from {{ source('ga4_properties', 's24_properties') }} 
),
accounts as (
    select id, name, account, platform, attribute, account_level_2 
    from {{ source('google_drive_clients', 'accounts') }} 
    where platform = 'ga4'
)

SELECT 
    properties.account_id,
    properties.account_display_name,
    properties.property_id,
    properties.property_display_name,
    accounts.attribute
from properties
left join accounts on accounts.account::text = properties.account_id::text