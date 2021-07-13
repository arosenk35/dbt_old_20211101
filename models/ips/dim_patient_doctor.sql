{{
  config({
      "materialized": "table",
      "post-hook": [
      after_commit("create index if not exists index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (patient_id)")
       ]})
}}
SELECT 
    distinct on (p.patient_id)
    p.patient_id,
	p.doctor_id
FROM  ips.prescription p 
order by 
    p.patient_id,
    case when p.doctor_id ='-1' then 9 else 1 end,
    p.changed_date desc