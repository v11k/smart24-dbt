{% macro get_company_name() %}
    {#- Attempt to split by Unix-like path separator, and if it fails, split by Windows path separator -#}
    {%- set path_parts = model.path.split('/') -%}
    {%- if path_parts | length == 1 -%}
        {%- set path_parts = model.path.split('\\') -%}
    {%- endif -%}
    {%- set company_name = path_parts[1] | trim | replace('\n', '') -%}  {#- Remove newline characters -#}
    {{- company_name -}}
{% endmacro %}
