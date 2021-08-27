{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_p_id on {{this.schema}}.{{this.name}} (patient_id)")]
  })
}}
SELECT   patient_id,
		 min(order_date) as first_order_date,
		 max(order_date) as last_order_date,
         count(distinct order_id)  as nbr_orders

FROM {{ ref('cs_fact_order_header') }} 
group by patient_id