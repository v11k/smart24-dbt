{% macro get_executive_platform_comparison_query(company_name) %}
with ga4_traffic as (
select
		"Date" as "date",
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
	group by 1,2
),
ga4_conversions as (
select
	    "Date" as "date",
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
	 group by 1,2
),
combined_data AS (
--FACEBOOK ADS
(select 
    fbads."Date" as "date",
    'Facebook Ads' as "platform",
    sum(fbads."Cost") as "cost",
    sum(fbads."Impressions") as "impressions",
    sum(fbads."Link clicks") as "clicks",
    sum(fbads."Website conversions") as "conversions",
    sum(ga4c.ga4_conversions) as ga4_conversions,
    sum(ga4c.ga4_conversion_value) as ga4_conversion_value,
    sum(ga4t.ga4_sessions) as ga4_sessions
from {{ ref("fbads_ads_" ~ company_name)}} fbads
left join (
    select 
        date, 
        ga4_sessions
    from ga4_traffic
    where platform = 'Facebook Ads'
) ga4t
    on ga4t.date = fbads."Date"
left join (
    select 
        date, 
        ga4_conversions,
        ga4_conversion_value
    from ga4_conversions
    where platform = 'Facebook Ads'
) ga4c
    on ga4c.date = fbads."Date"
group by 1,2
)
		
UNION
--GOOGLE ADS
(select 
    gads."Date" as "date",
	'Google Ads' as "platform",
	sum(gads."Cost") as "cost",
 	sum(gads."Impressions") as "impressions",
 	sum(gads."Clicks") as "clicks",
	sum(gads."Conversions") as "conversions",
 	sum(ga4c.ga4_conversions) as ga4_conversions,
 	sum(ga4c.ga4_conversion_value) as ga4_conversion_value,
 	sum(ga4t.ga4_sessions) as ga4_sessions
from {{ ref("gads_ads_" ~ company_name)}} gads
left join (
    select 
        date, 
        ga4_sessions
    from ga4_traffic
    where platform = 'Google Ads'
) ga4t
    on ga4t.date = gads."Date"
left join (
    select 
        date, 
        ga4_conversions,
        ga4_conversion_value
    from ga4_conversions
    where platform = 'Google Ads'
) ga4c
    on ga4c.date = gads."Date"
group by 1,2
)
		
UNION
--FACEBOOK INSIGHTS
(SELECT 
	fbi."Post creation date" as "date",
	'Organikus Facebook' as "platform",
	0 as "cost",
	sum(fbi."Post impressions") as "impressions",
	NULL as "clicks",
	0 as "conversions",
 	sum(ga4c.ga4_conversions) as ga4_conversions,
 	sum(ga4c.ga4_conversion_value) as ga4_conversion_value,
 	sum(ga4t.ga4_sessions) as ga4_sessions
FROM {{ ref("fbpages_posts_" ~ company_name)}} fbi
left join (
    select 
        date, 
        ga4_sessions
    from ga4_traffic
    where platform = 'Organikus Facebook'
) ga4t
    on ga4t.date = fbi."Post creation date"
left join (
    select 
        date, 
        ga4_conversions,
        ga4_conversion_value
    from ga4_conversions
    where platform = 'Organikus Facebook'
) ga4c
    on ga4c.date = fbi."Post creation date"
group by 1,2
)
		
UNION
--INSTAGRAM INSIGHTS
(select 
    i."Date" as "date",
	'Organikus Instagram' as "platform",
	0 as "cost",
 	sum(i."Media impressions") as "impressions",
 	0 as "clicks",
	0 as "conversions",
 	sum(ga4c.ga4_conversions) as ga4_conversions,
 	sum(ga4c.ga4_conversion_value) as ga4_conversion_value,
 	sum(ga4t.ga4_sessions) as ga4_sessions
from {{ ref("insta_posts_" ~ company_name)}} i
left join (
    select 
        date, 
        ga4_sessions
    from ga4_traffic
    where platform = 'Organikus Instagram'
) ga4t
    on ga4t.date = i."Date"
left join (
    select 
        date, 
        ga4_conversions,
        ga4_conversion_value
    from ga4_conversions
    where platform = 'Organikus Instagram'
) ga4c
    on ga4c.date = i."Date"
group by 1,2
)

UNION 
 --GOOGLE ANALYTICS 4 EGYÉB
SELECT
	ga4t.date,
	ga4t.platform,
	0 as "cost",
 	0 as "impressions",
 	0 as "clicks",
 	0 as "conversions",
	sum(coalesce(ga4c.ga4_conversions,0)) as ga4_conversions,
	sum(coalesce(ga4c.ga4_conversion_value,0)) as ga4_conversion_value,
	sum(coalesce(ga4t.ga4_sessions,0)) as ga4_sessions
FROM	
(   select 
        date, 
        ga4_sessions,
        platform
    from ga4_traffic
    where platform = 'Egyéb'
)  ga4t
left join (
    select 
        date, 
        ga4_conversions,
        ga4_conversion_value
    from ga4_conversions
    where platform = 'Egyéb'
) ga4c
    on ga4c.date = ga4t.date
group by 1,2


UNION

--MAILCHIMP
(select 
    mc."Date" as "date",
	'Email Marketing' as "platform",
	0 as "cost",
 	sum(mc."Unique opens") as "impressions",
 	sum(mc."Unique clicks") as "clicks",
	0 as "conversions",
 	sum(ga4c.ga4_conversions) as ga4_conversions,
 	sum(ga4c.ga4_conversion_value) as ga4_conversion_value,
 	sum(ga4t.ga4_sessions) as ga4_sessions
from {{ ref("mailchimp_campaigns_" ~ company_name)}} mc
left join (
    select 
        date, 
        ga4_sessions
    from ga4_traffic
    where platform = 'Email Marketing'
) ga4t
    on ga4t.date = mc."Date"
left join (
    select 
        date, 
        ga4_conversions,
        ga4_conversion_value
    from ga4_conversions
    where platform = 'Email Marketing'
) ga4c
    on ga4c.date = mc."Date"
group by 1,2)

)
,
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
        "platform", 
        SUM(cost) AS cost, 
        SUM(impressions) AS impressions, 
        SUM(clicks) AS clicks, 
        SUM(conversions) AS conversions, 
        SUM(ga4_conversions) AS ga4_conversions, 
        SUM(ga4_conversion_value) AS ga4_conversion_value, 
        SUM(ga4_sessions) AS ga4_sessions
    FROM combined_data
    GROUP BY "date", "platform"
),
cross_joined_data AS (
    -- Cross join the date range with accounts and platforms, ensuring every date has a row for every account-platform
    SELECT 
        cast(dr.date as date) as date,
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
        SELECT DISTINCT platform 
        FROM aggregated_combined_data
    ) ac
    LEFT JOIN aggregated_combined_data agg
    ON cast(dr.date as date) = agg.date
    AND ac.platform = agg.platform
)
SELECT
    cd1.date,
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
    ON cd1.platform = cd2.platform
    AND cd2.date = cd1.date - INTERVAL '30 DAYS'
ORDER BY cd1.date DESC, cd1.platform;

{% endmacro %}