{% set schema_name = get_mailchimp_schema() %}

{{ get_mailchimp_lists_query(schema_name) }}