with creative_ids as(
	select 
		distinct id as ad_id, 
		replace((creative -> 'id')::text, '"','') as creative_id 
	from {{ source('facebook_ads', 'ads') }} 
)

select distinct 
	cr.object_story_id as fbpages_id,
	crids.ad_id as fbads_id
from {{ source('facebook_ads', 'ad_creatives') }}  cr
inner join creative_ids crids on
	crids.creative_id = cr.id
where cr.object_story_id is not null