 {{
  config({
    "materialized": "table",
    "post-hook": [
    after_commit("create index  index_{{this.name}}_on_p_id on {{this.schema}}.{{this.name}} (practice_id)")
            ]
  })
  }}
  select 
  distinct on(m.practice_id)
	m.practice_id,	
  initcap(coalesce(v.clinic,v.address,firstname||' '||lastname)) as practice,
	firstname,
	lastname,
	email,
	zip,
	phone1,
	phone2,
	phone3,
	state,
	city,
	website,
	address,
	address2,
	country,
	dea_expiration_date,
	sln_expiration_date,
	dea,
	sln,
	case when active='Y' then 1 else 99 end as rank,
	case    when v.clinic is not null     then 1
          when address ilike '%vet%'    then 1
          when address ilike '%hosp%'   then 1
          when address ilike '%clinic%' then 1 
          when address ilike '%animal%' then 1
          when address ilike '%corpor%' then 1
          when address ilike '%pets%'   then 1
		  else 99
    end as rank_clinic
  FROM {{ ref('dim_practice_map') }}  m
  join {{ ref('dim_vet') }}  v on m.doctor_id=v.doctor_id
  order by 
  m.practice_id, rank asc,
  rank_clinic asc,
  v.created_date desc