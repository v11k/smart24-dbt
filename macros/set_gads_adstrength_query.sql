{% macro get_gads_adstrength_query(company_name) %}

with cleaned_ads as (
	SELECT 
		segments_date as "Date",
		(regexp_matches(ad_group_ad_ad_group, 'customers/(\d+)/'))[1] as "Account ID",
		ad_group_id as "Ad group ID",
		ad_group_ad_ad_id as "Ad ID",
		ad_group_ad_ad_type as "Ad type",
		ad_group_ad_status as "Ad status",
		ad_group_ad_ad_strength as "Ad strength",
		ad_group_ad_policy_summary_approval_status as "Ad approval status",
		ad_group_ad_ad_final_urls as "Final URL",
		sum(metrics_impressions) as "Impressions"
    FROM google_ads.ad_group_ad_legacy
	group by 1,2,3,4,5,6,7,8,9
)
select 
	ad.*,
	((('https://ads.google.com/aw/adgroups?__e='::text || acc.customer_id) || '&campaignId='::text) || gr.campaign_id || '&adGroupId='::text) || ad."Ad group ID" as "Ad group edit link",
	acc.customer_descriptive_name as "Account",
	gr.campaign_id as "Campaign ID"

from cleaned_ads ad
left join {{ ref("gads_accounts_with_attribute")}} acc
	on acc.customer_id::text = ad."Account ID"::text
left join (select distinct ad_group_id, ad_group_name, campaign_id from google_ads.ad_group) gr
	on gr.ad_group_id = ad."Ad group ID"
where acc.attribute = '{{ company_name }}'
    and "Ad strength" is not NULL
    and "Ad strength" <> 'UNSPECIFIED'


{% endmacro %}