{% macro get_fbpages_page_gender_query(company_name) %}

WITH extracted_values AS (
	SELECT
		split_part(id, '/', 1) as page_id,
	    key,
	    value::int AS reach,
	    CASE
	      WHEN key LIKE 'F.%' THEN 'Female'
	      WHEN key LIKE 'M.%' THEN 'Male'
	      WHEN key LIKE 'U.%' THEN 'Unknown'
	    END AS gender,
	    CASE
	      WHEN key LIKE '%.13-17' THEN '13-17'
	      WHEN key LIKE '%.18-24' THEN '18-24'
	      WHEN key LIKE '%.25-34' THEN '25-34'
	      WHEN key LIKE '%.35-44' THEN '35-44'
	      WHEN key LIKE '%.45-54' THEN '45-54'
	      WHEN key LIKE '%.55-64' THEN '55-64'
	      WHEN key LIKE '%.65+' THEN '65+'
	    END AS age
	FROM
    facebook_pages_custom.{{ company_name }}_page_insights,
    jsonb_each_text(values->0->'value')
	where name = 'page_impressions_by_age_gender_unique'
		and "period" = 'days_28'
)
SELECT
  ex.page_id,
  p.name,
  case when ex.gender = 'Female' then 'Nő'
	when ex.gender = 'Male' then 'Férfi'
	when ex.gender = 'Unknown' then 'Ismeretlen'
	else null end as gender,
  ex.age,
  SUM(ex.reach) AS reach
FROM
  extracted_values ex
left join facebook_pages_custom.{{ company_name }}_page p
	on p.id = ex.page_id
GROUP BY
  ex.gender, ex.age, ex.page_id, p.name
ORDER BY
  ex.age, ex.gender


{% endmacro %}