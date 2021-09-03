{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_drug_form on {{this.schema}}.{{this.name}} (drug_form)")]
  })
}}

select 
        distinct on(ips_drug_form)
        ips_drug_form as drug_form,
        image_path 
from {{ ref('cs_dim_product') }} 
where 
        nullif(ips_drug_form,'') is not null and 
        image_path is not null
