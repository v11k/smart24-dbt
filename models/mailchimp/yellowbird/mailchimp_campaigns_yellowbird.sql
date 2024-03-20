{% set schema_name = get_mailchimp_schema() %}


{{ get_mailchimp_campaigns_query(schema_name) }}