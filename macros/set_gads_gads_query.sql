{% macro get_gads_ads_query(company_name) %}

with cleaned_ads as (
	SELECT 
		segments_date as "Date",
		(regexp_matches(ad_group_ad_ad_group, 'customers/(\d+)/'))[1] as "Account ID",
		ad_group_id as "Ad group ID",
		ad_group_ad_ad_id as "Ad ID",
		replace(concat(upper("left"(ad_group_ad_ad_type::text, 1)), lower(SUBSTRING(ad_group_ad_ad_type FROM 2))), '_'::text, ' '::text) as "Ad type",
		ad_group_ad_status as "Ad status",
		ad_group_ad_ad_strength as "Ad strength",
		ad_group_ad_policy_summary_approval_status as "Ad approval status",
		ad_group_ad_ad_final_urls -> 0 as "Final URL",
		sum(metrics_impressions) as "Impressions",
		sum(metrics_clicks) as "Clicks",
		sum(metrics_cost_micros/1000000) as "Cost",
		sum(metrics_interactions) as "Interactions",
		sum(metrics_conversions) as "Conversions",
		sum(metrics_conversions_value) as "Total conversion value"
	FROM {{ source('google_ads', 'ad_group_ad_legacy') }}
	group by 1,2,3,4,5,6,7,8,9
)
select 
	ad.*,
	((('https://ads.google.com/aw/adgroups?__e='::text || acc.customer_id) || '&campaignId='::text) || gr.campaign_id || '&adGroupId='::text) || ad."Ad group ID" as "Ad group edit link",
	acc.customer_descriptive_name as "Account",
	gr.campaign_id as "Campaign ID",
	c.campaign_name as "Campaign name",
	c.campaign_advertising_channel_type as "Advertising channel type"

from cleaned_ads ad
left join {{ ref("gads_accounts_with_attribute")}} acc
	on acc.customer_id::text = ad."Account ID"::text
left join (select distinct ad_group_id, ad_group_name, campaign_id, segments_date from google_ads.ad_group) gr
	on gr.ad_group_id = ad."Ad group ID"
	and gr.segments_date= ad."Date"
left join (
	select distinct 
		campaign_id, 
		campaign_name, 
		replace(concat(upper("left"(campaign_advertising_channel_type::text, 1)), lower(SUBSTRING(campaign_advertising_channel_type FROM 2))), '_'::text, ' '::text) as campaign_advertising_channel_type
		from google_ads.campaign
) c
	on c.campaign_id = gr.campaign_id
where acc.attribute = '{{ company_name }}'


{% endmacro %}