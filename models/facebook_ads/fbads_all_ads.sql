-- models/facebook_ads/fbads_all_ads.sql

{% set fbads_models = [] %}

-- Define all models starting with "fbads_ads" as dependencies
{% for name in ['fbads_ads_*'] %}
  {{ config(materialized='view', depends_on=[name]) }}
  {% do fbads_models.append(name.replace('*', '')) %}
{% endfor %}

-- Now you can reference the models list
{% if fbads_models | length == 0 %}
  select null as placeholder -- Fallback in case no models match the pattern
{% else %}
  {% for model in fbads_models %}
    select * from {{ ref(model) }}
    {% if not loop.last %}
      UNION ALL
    {% endif %}
  {% endfor %}
{% endif %}
