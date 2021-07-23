{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (master_drug)")]
  })
  }}
  
SELECT distinct on (master_drug)
master_drug,
api_category,
(lower(controlled)='y')   as controlled,
(lower(common)='y')       as common
FROM {{ ref('api_category') }}