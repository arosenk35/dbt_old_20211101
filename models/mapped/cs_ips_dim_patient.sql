{{
  config({
    "materialized": "table",
    "post-hook": [
		after_commit("create index  index_{{this.name}}_on_pet_id on {{this.schema}}.{{this.name}} (patient_id)")]
  })
}}

SELECT  distinct on(cs.patient_id)
cs.*,

rank() OVER (
    PARTITION BY ips_patient_id 
    ORDER BY cs.rank ASC,last_order_date desc 
) as priority	
FROM {{ ref('cs_ips_der_patient_match') }}  cs

order by 
		cs.patient_id,
		rank asc
