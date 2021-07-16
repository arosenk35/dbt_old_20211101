 {{
  config({
    "materialized": "table",
    "post-hook": [
    after_commit("create index  index_{{this.name}}_on_pg on {{this.schema}}.{{this.name}} (practice)")
            ]
  })
  }}
  select distinct on(practice_group)
  practice_group as practice,
  country,
  state,
  city ,
  zip,
  phone1
  FROM {{ ref('dim_practice_map') }} 