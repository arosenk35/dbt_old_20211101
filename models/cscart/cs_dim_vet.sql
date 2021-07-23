{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_d_id on {{this.schema}}.{{this.name}} (doctor_id)")]
  })
}}
---- required for sln nbr & matching
SELECT   
		distinct on(cs.vet_data__id) 

		nullif(regexp_replace(lower(cs.vet_data__firstname||cs.vet_data__lastname),' ','','g'),'') as key_vet,
		nullif(regexp_replace(cs.phone,' |-|\(|\)','','g'),'') 			as key_phone,
		nullif(regexp_replace(cs.vet_data__sin,'[^0-9]+', '', 'g'),'') 	as key_sln,
		lower(cs.vet_data__email) 										as email, 
		nullif(regexp_replace(cs.vet_data__fax ,' |-|\(|\)','','g'),'') as fax, 
		initcap(cs.vet_data__firstname) 		as firstname,
		cs.vet_data__id 						as doctor_id,
		initcap(cs.vet_data__lastname) 			as lastname, 
		coalesce(initcap(cs.vet_data__firstname),'') || ' ' || coalesce(initcap(cs.vet_data__lastname),'') 	as vet,
		nullif(regexp_replace(cs.vet_data__phone,' |-|\(|\)','','g'),'') as phone, 
		nullif(cs.vet_data__sin,'-') as sln,
		ips.doctor_id 				as ips_doctor_id,
		ips.vet 					as ips_vet_name,
	    (cs.email ilike '%ggvcp%')  as ggvcp_vet
FROM cscart.orders cs

left join {{ ref('dim_vet') }} ips
on ( nullif(regexp_replace(lower(cs.vet_data__firstname||cs.vet_data__lastname),' ','','g'),'') = ips.key_vet)
or ( nullif(regexp_replace(cs.vet_data__sin,'[^0-9]+', '', 'g'),'') = ips.key_sln)
or ( nullif(regexp_replace(cs.phone,' |-|\(|\)','','g'),'') = ips.key_phone)
or cs.email = ips.email

order by cs.vet_data__id,
cs.timestamp desc,ips.created_date desc
