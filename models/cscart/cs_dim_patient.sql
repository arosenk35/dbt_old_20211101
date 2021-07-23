{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_pat_id on {{this.schema}}.{{this.name}} (account_id)"),
		after_commit("create index  index_{{this.name}}_on_pet_id on {{this.schema}}.{{this.name}} (patient_id)")]
  })
}}
SELECT  distinct on(coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id)|| coalesce(cs.pet_id,'') )

        coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id)|| coalesce(cs.pet_id,'')  as patient_id,
		coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id) 	as account_id,
		nullif(cs.vet_data__id,'') 						as doctor_id,
		initcap(nullif(cs.pet_data__name,'')) 			as patient_name,
		nullif(replace(cs.pet_data__dob,'-','/'),'/')   as dob,
		case  when cs.pet_data__sex ilike '%female%'  	then  'F'
          when cs.pet_data__sex ilike 'male%'     		then  'M'
          when cs.pet_data__sex ilike '%other%'   		then  'O'
    	end as sex,
		nullif(cs.pet_data__sex,'-') 					as sex,
		initcap(nullif(cs.pet_data__species,'-')) 		as species,
		initcap(nullif(cs.pet_data__breed,'-')) 		as breed,
		nullif(lower(cs.pet_data__weight),'-')	 		as weight,
		TIMESTAMP 'epoch' + timestamp::numeric * INTERVAL '1 second' as last_order_date,
		ips_account_id,
		dmp.patient_id as ips_patient_id
FROM cscart.orders cs
left join {{ ref('cs_dim_owner') }} o on o.account_id=coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id)
left join {{ ref('dim_patient') }} dmp on dmp.key_pet=lower(cs.pet_data__name)
and o.ips_account_id=dmp.account_id and nullif(cs.pet_data__name,'') is not null
order by coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id)|| coalesce(cs.pet_id,'') ,
timestamp desc