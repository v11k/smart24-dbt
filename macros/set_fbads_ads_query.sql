{% macro get_fbads_ads_query(company_name) %}
WITH extracted_actions AS (
  SELECT
	ad_id,
	date_start as date,
    SUM((action->>'value')::int) FILTER (WHERE action->>'action_type' = 'post_engagement') AS post_engagements,
    SUM((action->>'value')::int) FILTER (WHERE action->>'action_type' = 'video_view') AS video_views,
	SUM((action->>'7d_click')::int) FILTER (WHERE action->>'action_type' like '%offsite_conversion%') as web_conversions,
	SUM((action->>'7d_click')::int) FILTER (WHERE action->>'action_type' = 'omni_purchase') as purchases,
	SUM((action->>'7d_click')::int) FILTER (WHERE action->>'action_type' = 'lead') as leads,
	SUM((action->>'7d_click')::int) FILTER (WHERE action->>'action_type' like '%messag%') as messages
  FROM
    {{ source('facebook_ads', 'ads_insights') }},
    jsonb_array_elements(actions) AS action
  group by 1,2
),
creative_ids as(
	select 
		distinct id as ad_id, 
		replace((creative -> 'id')::text, '"','') as creative_id 
	from {{ source('facebook_ads', 'ads') }}
)
select 
	ai.date_start as "Date",
	ai.account_name as "Account",
	ai.account_id as "Account ID",
	ai.campaign_id as "Campaign ID",
	ai.campaign_name as "Campaign name",
	ai.adset_name as "Ad set name",
	ai.adset_id as "Ad set ID",
	ai.ad_id as "Ad ID",
	ai.ad_name as "Ad name",
	cre.url_tags as "Ad url tags",
	ai.objective as "Campaign objective",
	concat('https://www.facebook.com/adsmanager/manage/adsets?act=', ai.account_id, '&selected_campaign_ids=', ai.campaign_id) as "Campaign edit link",
	concat('https://www.facebook.com/adsmanager/manage/ads?act=', ai.account_id, '&selected_campaign_ids=', ai.campaign_id, '&selected_adset_ids=', ai.adset_id) as "Ad set edit link",
	coalesce(cre.image_url, cre.thumbnail_url) as "Ad creative image URL",
	c.configured_status as "Campaign configured status",
	cre.name as "Creative name",
	cre.title as "Creative title",
	cre.body as "Ad body",	
	coalesce(ai.reach,0) as "Reach",
	coalesce(ai.impressions,0) as "Impressions",
	coalesce(ai.inline_link_clicks,0) as "Link clicks",
	coalesce(ex.web_conversions,0) as "Website conversions",
	coalesce(ai.spend,0) as "Cost",
	coalesce(ex.post_engagements,0) as "Post engagements",
	coalesce(ex.video_views,0) as "Video views",
	coalesce(ex.purchases,0) as "Purchases",
	coalesce(ex.leads,0) as "Leads",
	coalesce(ex.messages,0) as "New messaging conversations",
	CASE
            WHEN ai.objective::text ~~ '%CONVERSION%'::text THEN 'Konverziók'::character varying
            WHEN ai.objective::text ~~ '%ENGAGEMENT%'::text THEN 'Post interakció'::character varying
            WHEN ai.objective::text ~~ '%VIEWS%'::text THEN 'Videó megtekintés'::character varying
            WHEN ai.objective::text ~~ '%SALES%'::text THEN 'Eladás'::character varying
            WHEN ai.objective::text ~~ '%CLICKS%'::text OR ai.objective::text ~~ '%TRAFFIC%'::text THEN 'Forgalom'::character varying
            WHEN ai.objective::text ~~ '%REACH%'::text OR ai.objective::text ~~ '%AWARENESS%'::text THEN 'Elérés'::character varying
            WHEN ai.objective::text ~~ '%LEAD%'::text THEN 'Leadek'::character varying
            WHEN ai.objective::text ~~ '%MESSAGE%'::text THEN 'Üzenetek'::character varying
            ELSE ai.objective
        END AS "Kampány cél",
        case when cre.object_story_id is null then 'Nem boost' else 'Boost' end as boost
	
from {{ source('facebook_ads', 'ads_insights') }} ai
left join extracted_actions ex
	on ex.date = ai.date_start
	and ex.ad_id = ai.ad_id
left join {{ source('facebook_ads', 'campaigns') }} c
	on c.id = ai.campaign_id

left join {{ source('facebook_ads', 'ad_sets') }} adsets
	on adsets.id = ai.adset_id
left join creative_ids as creid
	on creid.ad_id = ai.ad_id
left join {{ source('facebook_ads', 'ad_creatives') }} cre
	on cre.id = creid.creative_id
left join {{ ref("fbads_accounts_with_attribute")}} acc
	on acc.account_id::text = ai.account_id::text
where acc.attribute = '{{ company_name }}'
    and ai.impressions is not null
	and ai.impressions > 0
order by ai.date_start 

{% endmacro %}