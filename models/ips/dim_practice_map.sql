{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (practice)"),
      after_commit("create index  index_{{this.name}}_on_id_group on {{this.schema}}.{{this.name}} (practice_group)"),
      ]
  })
  }}

  with max_created as (
    select doctor_id,max(created_date) as max_created_date
    from ips.prescription
    group by doctor_id
  ),

  
  vet as(
    SELECT 
    coalesce(nullif(address,''),note)   as practice,
    nullif(address,'')                  as address,
    z.zipid::text                       as zip,
    phone11||'-'||phone12||'-'||phone13 as phone1,
    coalesce(z.country,'USA')           as country,
    coalesce(z.state,'CA')              as state,
	  z.city,
    sv.max_created_date

	FROM ips.doctor_master dm
  join max_created sv on dm.srno=sv.doctor_id
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
                max_created_date desc,
							  case when btrim(practice) is null then 99
							  when address ilike '%vet%' then 2
							  when address ilike '%hosp%' then 1 
							  when address ilike '%clinic%' then 3
                when address ilike '%animal%' then 4
                when address ilike '%center%' then 5
                when address ilike '%pets%' then 6
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
