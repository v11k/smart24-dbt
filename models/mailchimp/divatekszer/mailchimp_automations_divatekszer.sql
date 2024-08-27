{% set schema_name = get_mailchimp_schema() %}

{{ get_mailchimp_automations_query(schema_name) }}