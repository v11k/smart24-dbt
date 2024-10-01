{% macro generate_ga4_union_ecom_roi(company_name) %}

with fbads as (
	SELECT
    	"Date" as date,
    	(regexp_matches("Ad url tags", 'utm_campaign=([^&]+)'))[1] AS utm_campaign,
    	SUM("Cost") AS cost
	FROM {{ ref("fbads_ads_" ~ company_name)}}
	GROUP BY "Date", utm_campaign)
,
gads as (
	SELECT
		"Date" as date,
		"Campaign name" as utm_campaign,
		sum("Cost") as cost
	from {{ ref("gads_campaigns_" ~ company_name)}}
	group by 1,2
),
ga4_purchase as (
	SELECT
		"Date" as date,
		"Event campaign name" as utm_campaign,
        "Source / medium" as source_medium,
		sum("Conversions") as purchases,
		sum("Event value") as purchase_value
	FROM {{ ref("ga4_conversions_" ~ company_name)}}
	WHERE "Event name" = 'purchase'
	GROUP BY 1,2,3
)
SELECT
	coalesce(ga4.date, gads.date, fbads.date) as date,
	ga4.source_medium,
	coalesce(ga4.utm_campaign, gads.utm_campaign, fbads.utm_campaign) as campaign,
	coalesce(fbads.cost,0) as fbads_cost,
	coalesce(gads.cost,0) as gads_cost,
	coalesce(ga4.purchases,0) as purchases,
	coalesce(ga4.purchase_value,0) as purchase_value,
	case when ga4.source_medium like '%google%cpc%' then ga4.purchase_value else 0 end as google_purchase_value,
	case when ga4.source_medium like '%facebook%cpc%' then ga4.purchase_value else 0 end as facebok_purchase_value
FROM ga4_purchase ga4
full outer join fbads on fbads.date = ga4.date
	and fbads.utm_campaign = ga4.utm_campaign
full outer join gads on gads.date = ga4.date
	and gads.utm_campaign = ga4.utm_campaign

{% endmacro %}
