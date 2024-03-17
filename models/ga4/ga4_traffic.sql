select
	TO_DATE(tr.date, 'YYYYMMDD') as "Date",
	p.account_name as "Account name",
	p.account_id as "Account ID",
	p.property_name as "GA4 property",
	p.property_id as "GA4 property ID",
	tr."sessionSourceMedium" as "Session source / medium",
	tr."sessionDefaultChannelGroup" as "Session default channel grouping",
	tr."sessionCampaignName" as "Session campaign name",
	tr."sessionManualTerm" as "Session manual term",
	tr."sessionManualAdContent" as "Session manual ad content",
	tr."activeUsers" as "Active users",
	tr."sessions" as "Sessions",
	tr."newUsers" as "New users",
	tr."engagedSessions" as "Engaged sessions",
	tr."screenPageViews" as "Views",
	tr."userEngagementDuration" as "Total user engagement duration (sec)"
from ga4.s24_traffic tr
left join ga4.acc_property p
ON p.property_id::text = tr.property_id::text
order by "Date" desc