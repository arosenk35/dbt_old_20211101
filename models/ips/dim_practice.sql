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
  m.practice,
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
  organization_id
  FROM {{ ref('dim_practice_map') }}  m
  join {{ ref('dim_vet') }}  v on m.doctor_id=v.doctor_id
  left join {{ ref('dim_organization') }} org on v.clinic ilike '%'||org.name||'%'