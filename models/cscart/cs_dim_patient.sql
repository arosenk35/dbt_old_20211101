{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_pat_id on {{this.schema}}.{{this.name}} (account_id)"),
		after_commit("create index  index_{{this.name}}_on_pet_id on {{this.schema}}.{{this.name}} (patient_id)"),
		after_commit("create index  index_{{this.name}}_on_key on {{this.schema}}.{{this.name}} using gist (key_patient)")]
  })
}}

SELECT  distinct on(cs.patient_id)
*	
FROM {{ ref('cs_der_patient_match') }}  cs

order by 
		cs.patient_id,
		rank asc
