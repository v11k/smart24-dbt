{% macro get_mailchimp_schema() %}
    {# Attempt to split by Unix-like path separator, and if it fails, split by Windows path separator #}
    {% set path_parts = model.path.split('/') %}
    {% if path_parts | length == 1 %}
        {% set path_parts = model.path.split('\\') %}
    {% endif %}
    {% set company_name = path_parts[1] %}
    {% set schema_name = 'mailchimp_' ~ company_name %}
    {{ schema_name }}
{% endmacro %}
