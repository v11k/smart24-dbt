{% macro get_executive_platform_comparison_query(company_name) %}
with combined_data AS (
--FACEBOOK ADS
(select 
    "Date" as "date",
    "Account" as "account",
    "Account ID"::text as "account_id",
    'Facebook Ads' as "platform",
    sum("Cost") as "cost",
    sum("Impressions") as "impressions",
    sum("Link clicks") as "clicks",
    sum("Website conversions") as "conversions",
    0 as ga4_conversions,
    0 as ga4_conversion_value,
    0 as ga4_sessions
from {{ ref("fbads_ads_" ~ company_name)}}
group by 1,2,3,4
)
		
UNION
--GOOGLE ADS
(select 
    "Date" as "date",
    "Account" as "account",
	"Account ID"::text as "account_id",
	'Google Ads' as "platform",
	sum("Cost") as "cost",
 	sum("Impressions") as "impressions",
 	sum("Clicks") as "clicks",
	sum("Conversions") as "conversions",
 	0 as "ga4_conversions",
 	0 as ga4_conversion_value,
 	0 as ga4_sessions
from {{ ref("gads_ads_" ~ company_name)}}
group by 1,2,3,4
)	
		
UNION
--FACEBOOK INSIGHTS
(SELECT 
	"Post creation date" as "date",
    "Page name" as "account",
 	"Page ID"::text as "account_id",
	'Organikus Facebook' as "platform",
	0 as "cost",
	sum("Post impressions") as "impressions",
	NULL as "clicks",
	0 as "conversions",
 	0 as "ga4_conversions",
 	0 as ga4_conversion_value,
 	0 as ga4_sessions
FROM {{ ref("fbpages_posts_" ~ company_name)}}
group by 1,2,3,4
 )
		
UNION
--INSTAGRAM INSIGHTS
(select 
    "Date" as "date",
	"Name" as "account",
 	"User ID"::text as "account_id",
	'Organikus Instagram' as "platform",
	0 as "cost",
 	sum("Media impressions") as "impressions",
 	0 as "clicks",
	0 as "conversions",
 	0 as "ga4_conversions",
 	0 as ga4_conversion_value,
 	0 as ga4_sessions
from {{ ref("insta_posts_" ~ company_name)}}
group by 1,2,3,4
)

UNION 
 --GOOGLE ANALYTICS 4
SELECT
	ga4t.date,
	ga4t.account,
	ga4t.account_id,
	ga4t.platform,
	0 as "cost",
 	0 as "impressions",
 	0 as "clicks",
 	0 as "conversions",
	coalesce(ga4c.ga4_conversions,0) as ga4_conversions,
	coalesce(ga4c.ga4_conversion_value,0) as ga4_conversion_value,
	coalesce(ga4t.ga4_sessions,0) as ga4_sessions
FROM	
	(select
		"Date" as "date",
		"Account name" as "account",
	 	"Account ID" as "account_id",
		case when "Session source / medium" like '%facebook%cpc%' then 'Facebook Ads'
			when "Session source / medium" like '%google%cpc%' then 'Google Ads'
			when "Session source / medium" like '%instagram%referral%' then 'Organikus Instagram'
			when "Session source / medium" like '%facebook%referral%' then 'Organikus Facebook'
			when "Session source / medium" like '%google%organic%' or "Session source / medium" like '%google%referral%' then 'Organikus Google keresés'
			when "Session source / medium" like '%linkedin%' then 'LinkedIn'
			when "Session default channel grouping" like '%Email%' then 'Email Marketing'
			else 'Egyéb'
		    end as "platform",
		sum("Sessions") as ga4_sessions
 	from {{ ref("ga4_traffic_" ~ company_name)}}
	group by 1,2,3,4
	)  ga4t
left join 
	 (select
	    "Date" as "date",
		"Account name" as "account",
	  	"Account ID" as "account_id",
		case when "Source / medium" like '%facebook%cpc%' then 'Facebook Ads'
            when "Source / medium" like '%google%cpc%' then 'Google Ads'
            when "Source / medium" like '%instagram%referral%' then 'Organikus Instagram'
            when "Source / medium" like '%facebook%referral%' then 'Organikus Facebook'
            when "Source / medium" like '%google%organic%' or "Source / medium" like '%google%referral%' then 'Organikus Google keresés'
            when "Source / medium" like '%linkedin%' then 'LinkedIn'
            when "Source / medium" like '%Email%' then 'Email Marketing'
            else 'Egyéb'
		    end as "platform",
		SUM("Conversions") as ga4_conversions,
		SUM("Event value") as ga4_conversion_value
	 from {{ ref("ga4_conversions_" ~ company_name)}}
	 group by 1,2,3,4
	) ga4c
on ga4c.date = ga4t.date and ga4c.account= ga4t.account and ga4t.platform = ga4c.platform

UNION

--MAILCHIMP
(select 
    "Date" as "date",
	'Email marketing account' as "account",
 	'Mailchimp ID' as "account_id",
	'Email Marketing' as "platform",
	0 as "cost",
 	sum("Unique opens") as "impressions",
 	sum("Unique clicks") as "clicks",
	0 as "conversions",
 	0 as "ga4_conversions",
 	0 as ga4_conversion_value,
 	0 as ga4_sessions
from {{ ref("mailchimp_campaigns_" ~ company_name)}}
group by 1,2,3,4)
),
date_range AS (
    -- Same as before: Generate a date range for all dates
    SELECT GENERATE_SERIES(
        (SELECT MIN("date") FROM combined_data),
        (SELECT MAX("date") FROM combined_data),
        '1 day'::INTERVAL
    ) AS date
),
-- Aggregate the combined_data to make sure each date-account-platform has a single row
aggregated_combined_data AS (
    SELECT 
        "date", 
        "account", 
        "account_id", 
        "platform", 
        SUM(cost) AS cost, 
        SUM(impressions) AS impressions, 
        SUM(clicks) AS clicks, 
        SUM(conversions) AS conversions, 
        SUM(ga4_conversions) AS ga4_conversions, 
        SUM(ga4_conversion_value) AS ga4_conversion_value, 
        SUM(ga4_sessions) AS ga4_sessions
    FROM combined_data
    GROUP BY "date", "account", "account_id", "platform"
),
cross_joined_data AS (
    -- Cross join the date range with accounts and platforms, ensuring every date has a row for every account-platform
    SELECT 
        dr.date,
        ac.account,
        ac.account_id,
        ac.platform,
        COALESCE(agg.cost, 0) AS cost,
        COALESCE(agg.impressions, 0) AS impressions,
        COALESCE(agg.clicks, 0) AS clicks,
        COALESCE(agg.conversions, 0) AS conversions,
        COALESCE(agg.ga4_conversions, 0) AS ga4_conversions,
        COALESCE(agg.ga4_conversion_value, 0) AS ga4_conversion_value,
        COALESCE(agg.ga4_sessions, 0) AS ga4_sessions
    FROM date_range dr
    CROSS JOIN (
        SELECT DISTINCT account, account_id, platform 
        FROM aggregated_combined_data
    ) ac
    LEFT JOIN aggregated_combined_data agg
    ON dr.date = agg.date
    AND ac.account = agg.account
    AND ac.account_id = agg.account_id
    AND ac.platform = agg.platform
)
SELECT
    cd1.date,
    cd1.account,
    cd1.platform,
    cd1.cost,
    cd1.impressions,
    COALESCE(cd1.clicks, 0) AS clicks,
    cd1.conversions,
    cd1.ga4_conversions,
    cd1.ga4_conversion_value,
    cd1.ga4_sessions,
    -- Add columns for metrics 30 days ago from the joined table (cd2)
    COALESCE(cd2.cost, 0) AS cost_30_days_ago,
    COALESCE(cd2.impressions, 0) AS impressions_30_days_ago,
    COALESCE(cd2.clicks, 0) AS clicks_30_days_ago,
    COALESCE(cd2.conversions, 0) AS conversions_30_days_ago,
    COALESCE(cd2.ga4_conversions, 0) AS ga4_conversions_30_days_ago,
    COALESCE(cd2.ga4_conversion_value, 0) AS ga4_conversion_value_30_days_ago,
    COALESCE(cd2.ga4_sessions, 0) AS ga4_sessions_30_days_ago
FROM cross_joined_data cd1
LEFT JOIN cross_joined_data cd2
    ON cd1.account = cd2.account
    AND cd1.account_id = cd2.account_id
    AND cd1.platform = cd2.platform
    AND cd2.date = cd1.date - INTERVAL '30 DAYS'
ORDER BY cd1.date DESC, cd1.account, cd1.platform;

{% endmacro %}