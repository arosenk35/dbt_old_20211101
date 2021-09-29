{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (practice)"),
      after_commit("create index  index_{{this.name}}_on_id_group on {{this.schema}}.{{this.name}} (practice_group)"),
      ]
  })
  }}
  
  with vet as(
    SELECT 
    coalesce(nullif(address,''),note)   as practice,
    nullif(address,'')                  as address,
    z.zipid::text                       as zip,
    phone11||phone12||phone13           as phone1,
    coalesce(z.country,'USA')           as country,
    coalesce(z.state,'CA')              as state,
	  z.city,
    dm.created_date,
    active

	FROM ips.doctor_master dm
  left join ips.zip_master z on dm.zip = z.srno
  )
  
 select distinct on(practice)
  practice,
  country,
  state,
  city ,
  zip,
  phone1,
  first_value(practice) OVER (PARTITION BY state,city,zip,phone1 order by 
                case when active='Y' then 1 else 99 end,
                created_date asc,                
							  case when btrim(practice) is null then 99
							  when address ilike '%vet%' then 2
							  when address ilike '%hosp%' then 1 
							  when address ilike '%clinic%' then 3
                when address ilike '%animal%' then 4
                when address ilike '%center%' then 5
                when address ilike '%corpor%' then 6
                when address ilike '%pets%' then 7
							  else 88
							  end
							  ) practice_group
   FROM vet

 where nullif(btrim(practice),' ') is not null
 and length(practice)>2

 union all 

 select
 'Unknown',
 null,
 null,
 null,
 null,
 null,
 'Unknown'
