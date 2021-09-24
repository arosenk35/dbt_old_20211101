{{
  config({
    "materialized": "table",
    "post-hook": [
    after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (order_id)")]
  })
}}
---track last known rxno for cscart order
select 
        distinct on (cs_lod.order_id)
        cs_lod.order_id,
        o.rxno,
        o.last_dispense_date 
from {{ ref('der_refill_status') }}  o
    join {{ ref('cs_ips_dim_patient') }}   cs_dmp 	        on cs_dmp.ips_patient_id=o.patient_id and priority=1
    join {{ ref('cs_ips_dim_last_order_drug') }}  cs_lod    on cs_lod.patient_id=cs_dmp.patient_id and cs_lod.ips_drug_id=o.drug_id
    order by    cs_lod.order_id,
                o.last_dispense_date
	
