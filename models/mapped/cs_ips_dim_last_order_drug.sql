{{
  config({
    "materialized": "table",
    "post-hook": [
    after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (patient_id,ips_drug_id)")]
  })
}}
SELECT  
    distinct on(patient_id,ips_drug_id)
    patient_id,
    o.order_id,
    o.order_date,
    ips_drug_id,
    p.product,
    p.product_id

FROM    {{ ref('cs_fact_order_header') }} o
        join {{ ref('cs_fact_order_lines') }} l on o.order_id=l.order_id
        join {{ ref('cs_ips_dim_product') }} p on l.product_id=p.product_id
where ips_drug_id is not null
order by 
patient_id,
ips_drug_id,
o.order_date desc
