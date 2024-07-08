{% macro get_gads_keywords_query(company_name) %}

with cleaned_keywords as (
	SELECT 
		segments_date as "Date",
		customer_descriptive_name as "Account",
		customer_id as "Account ID",
		campaign_id as "Campaign ID",
		ad_group_id as "Ad group ID",
		ad_group_criterion_keyword_text as "Keyword",
		ad_group_criterion_criterion_id as "Keyword ID",
		ad_group_criterion_keyword_match_type as "Keyword match type",
		metrics_historical_quality_score as "Historical quality score",
		sum(metrics_impressions) as "Impressions",
		sum(metrics_clicks) as "Clicks",
		sum(metrics_cost_micros/1000000) as "Cost",
		sum(metrics_interactions) as "Interactions",
		sum(metrics_conversions) as "Conversions",
		sum(metrics_conversions_value) as "Total conversion value"
	FROM {{ source('google_ads', 'keyword_view') }}
	group by 1,2,3,4,5,6,7,8,9

),
campaign as 
(
select distinct 
	campaign_id,
	campaign_name,
	campaign_status
from {{ source('google_ads', 'campaign') }}
),
ad_group as (

select distinct 
	ad_group_id,
	ad_group_name,
	ad_group_status
from {{ source('google_ads', 'ad_group') }}
),
crit as (
SELECT distinct
	ad_group_criterion_criterion_id,
	ad_group_id,
	ad_group_criterion_status,
	ad_group_criterion_system_serving_status,
	ad_group_criterion_quality_info_quality_score,
	ad_group_criterion_quality_info_search_predicted_ctr,
	ad_group_criterion_quality_info_post_click_quality_score,
	ad_group_criterion_quality_info_creative_quality_score
FROM {{ source('google_ads', 'ad_group_criterion') }}
where ad_group_criterion_type = 'KEYWORD'
)

SELECT
	k.*,
	c.campaign_name as "Campaign name",
	c.campaign_status as "Campaign status",
	ag.ad_group_name as "Ad group name",
 	ag.ad_group_status as "Ad group status",
	((('https://ads.google.com/aw/adgroups?__e='::text || k."Account ID") || '&campaignId='::text) || c.campaign_id || '&adGroupId='::text) || ag.ad_group_id as "Ad group edit link",
	crit.ad_group_criterion_status as "Keyword status",
	crit.ad_group_criterion_quality_info_quality_score as "Quality score",
	crit.ad_group_criterion_quality_info_creative_quality_score as "Creative quality score",
	crit.ad_group_criterion_quality_info_post_click_quality_score as "Post-click quality score",
	crit.ad_group_criterion_quality_info_search_predicted_ctr as "Search predicted CTR"
FROM cleaned_keywords k
	
LEFT JOIN campaign c
	ON c.campaign_id = k."Campaign ID"
LEFT JOIN ad_group ag
	ON ag.ad_group_id = k."Ad group ID"
LEFT JOIN crit
	ON crit.ad_group_id = k."Ad group ID"
	and crit.ad_group_criterion_criterion_id = k."Keyword ID"
left join {{ ref("gads_accounts_with_attribute")}} acc
	on acc.customer_id::text = k."Account ID"::text
where acc.attribute = '{{ company_name }}'


{% endmacro %}