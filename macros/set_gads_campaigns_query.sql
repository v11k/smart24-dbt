{% macro get_gads_campaigns_query(company_name) %}

with cleaned_campaigns as (
	SELECT 
		segments_date as "Date",
		campaign_name as "Campaign name",
		campaign_id "Campaign ID",
		campaign_status as "Campaign status",
		(regexp_matches(campaign_base_campaign, 'customers/(\d+)/'))[1] as "Account ID",
		replace(concat(upper("left"(campaign_advertising_channel_type::text, 1)), lower(SUBSTRING(campaign_advertising_channel_type FROM 2))), '_'::text, ' '::text) as "Advertising channel type",
		campaign_start_date as "Start date",
		campaign_end_date as "End date",
		case when campaign_optimization_score = 0 then NULL else campaign_optimization_score end as "Optimization score",
		sum(metrics_impressions) as "Impressions",
		sum(metrics_conversions) as "Conversions",
		sum(metrics_cost_micros/1000000) as "Cost",
		sum(metrics_clicks) as "Clicks",
		sum(metrics_interactions) as "Interactions",
		sum(metrics_conversions_value) as "Total conversion value"
	FROM {{ source('google_ads', 'campaign') }}
	group by 1,2,3,4,5,6,7,8,9
)
select 
	c.*,
	(('https://ads.google.com/aw/adgroups?__e='::text || acc.customer_id) || '&campaignId='::text) || c."Campaign ID" as "Campaign edit link",
	acc.customer_descriptive_name as "Account"
from cleaned_campaigns c
left join {{ ref("gads_accounts_with_attribute")}} acc
	on acc.customer_id::text = c."Account ID"::text

where acc.attribute = '{{ company_name }}'

{% endmacro %}