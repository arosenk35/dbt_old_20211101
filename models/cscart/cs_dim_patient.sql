{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_pat_id on {{this.schema}}.{{this.name}} (account_id)"),
		after_commit("create index  index_{{this.name}}_on_pet_id on {{this.schema}}.{{this.name}} (patient_id)")]
  })
}}
SELECT  distinct on(cs.patient_id)
		cs.patient_id,
		cs.account_id,
		cs.doctor_id,
		cs.patient_name,
		cs.key_patient,
		cs.firstname,
		cs.lastname,
		cs.dob,
		cs.sex,
		cs.species,
		cs.weight,
		cs.last_order_date,
		o.ips_account_id,
		dmp.patient_id as ips_patient_id,
		case 
			when not dmp.active 
			then 99 
			when (
					dmp.key_patient=cs.key_patient_species
						and o.ips_account_id=dmp.account_id
					)
					then 1
			when (
					dmp.key_patient like cs.key_patient||'%'
					and o.ips_account_id=dmp.account_id 
					)
			then 2
			else 88 
		end as rank
		
FROM {{ ref('cs_der_patient') }} cs
left join {{ ref('cs_dim_owner') }} o on o.account_id=cs.account_id
left join {{ ref('dim_patient') }} dmp 
		on 	(
				dmp.key_patient=cs.key_patient_species
					and o.ips_account_id=dmp.account_id
				)
			or 
			(
				dmp.key_patient like cs.key_patient||'%'
				and o.ips_account_id=dmp.account_id 
				)
where cs.patient_name is not null
order by 
		cs.patient_id,
		rank asc
