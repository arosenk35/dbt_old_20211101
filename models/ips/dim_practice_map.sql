   {{
  config({
    "materialized": "table",
    "post-hook": [
    after_commit("create index  index_{{this.name}}_on_p_id on {{this.schema}}.{{this.name}} (practice_id)"),
    after_commit("create index  index_{{this.name}}_on_d_id on {{this.schema}}.{{this.name}} (doctor_id)")
            ]
  })
  }}
  with
  
  vet as(
    SELECT 
      
      case  when nullif(address,'') is null then
              regexp_replace(replace(replace(replace(replace(replace(lower(note),'street','st'),'drive','dr'),'avenue','ave'),'road','rd'),'floor','fl'),'\#|\''|\`| |\,|\&|\.|-|','','g')
            else
              regexp_replace(replace(replace(replace(replace(replace(lower(address),'street','st'),'drive','dr'),'avenue','ave'),'road','rd'),'floor','fl'),'\#|\''|\`| |\,|\&|\.|-|','','g')
      end as fuzzy_key,   

      btrim(coalesce(nullif(address,''),note))  as practice,
      nullif(address,'')                  as address,
      z.zipid::text                       as zip,
      phone11||phone12||phone13           as phone1,
      coalesce(z.country,'USA')           as country,
      coalesce(z.state,'CA')              as state,
      z.city,
      dm.created_date,
      active,
      dm.srno as doctor_id,
	    prm.practice_id as practice_id_map

  FROM ips.doctor_master dm
	left join {{ ref('practice_map') }}  prm on    dm.srno = prm.doctor_id
  left join ips.zip_master z on dm.zip = z.srno
  where  exists (select 'x' from ips.prescription p where dm.srno=p.doctor_id and p.office_id=2)
	--  ignore office_id it lies!
  ),
  
  fuzzy as (select 
        first_value(doctor_id) OVER (PARTITION BY state,city,zip,phone1 order by 
                case when active='Y' then 1 else 99 end,                
				        case when practice_id_map is not null then 1 else 99 end,
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
                ) first_doctor_id,	

			          doctor_id,                

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
                ) first_practice,
                first_value(practice_id_map) OVER (PARTITION BY state,city,zip,phone1 order by 
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
                ) first_practice_id,
                practice_id_map
 
   FROM vet
   order by fuzzy_key
   )

  select     
      coalesce(first_practice_id, first_doctor_id+1000000) as practice_id,
      doctor_id,
      first_practice as practice,
      'fuzzy' as type 
    from fuzzy 
  where practice_id_map is null

  union all

  (select   
      m.practice_id,	
      m.doctor_id,
      m.practice,
      'csv' as type
  FROM {{ ref('practice_map') }}  m
  )