select 
	'Income' as source_type,
	cast(replace(honap,'.','-') as date) as date,
	ugyfel as source_name,
	cast(osszeg as numeric) as amount
from "google_drive"."Bevetelek"

UNION
	
select 
	'Facebook' as source_type,
	cast(replace(replace(kelte,'.','-'),' ','') as date) as date,
	partner as source_name,
	cast(brutto as numeric) as amount
from "google_drive"."Facebook"

UNION
	
select 
	'Google' as source_type,
	cast(replace(replace(kelte,'.','-'),' ','') as date) as date,
	partner as source_name,
	cast(brutto as numeric) as amount
from "google_drive"."Google"

UNION
	
select 
	'Server' as source_type,
	cast(replace(replace(kelte,'.','-'),' ','') as date) as date,
	partner as source_name,
	cast(brutto as numeric) as amount
from "google_drive"."Rackforest"

UNION
	
select 
	'Server' as source_type,
	cast(replace(replace(kelte,'.','-'),' ','') as date) as date,
	partner as source_name,
	cast(brutto as numeric) as amount
from "google_drive"."Litvan_server"

UNION
	
select 
	'ETL' as source_type,
	cast(replace(replace(kelte,'.','-'),' ','') as date) as date,
	partner as source_name,
	cast(brutto as numeric) as amount
from "google_drive"."Supermetrics"

UNION
	
select 
	'ETL' as source_type,
	cast(replace(replace(kelte,'.','-'),' ','') as date) as date,
	partner as source_name,
	cast(brutto as numeric) as amount
from "google_drive"."Fivetran"

UNION
	
select 
	'SSL' as source_type,
	cast(replace(replace(honap,'.','-'),' ','') as date) as date,
	megnevezes as source_name,
	cast(osszeg as numeric) as amount
from "google_drive"."SSL"

UNION
	
select 
	'Egyéb' as source_type,
	cast(replace(replace(honap,'.','-'),' ','') as date) as date,
	megnevezes as source_name,
	cast(osszeg as numeric) as amount
from "google_drive"."Egyeb"