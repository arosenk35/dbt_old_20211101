{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (zip)")]
  })
  }}
  
SELECT distinct on (zip)
zip,
territory
FROM {{ ref('sales_territories') }}