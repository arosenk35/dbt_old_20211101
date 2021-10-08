   {{
  config({
    "materialized": "table",
    "post-hook": [
    after_commit("create index  index_{{this.name}}_on_p_id on {{this.schema}}.{{this.name}} (practice_id)"),
    after_commit("create index  index_{{this.name}}_on_d_id on {{this.schema}}.{{this.name}} (doctor_id)")
            ]
  })
  }}
  select   
	m.practice_id,	
  m.doctor_id,
  m.practice
  FROM {{ ref('practice_map') }}  m