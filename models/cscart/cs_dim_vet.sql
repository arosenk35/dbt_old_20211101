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

		nullif(regexp_replace(lower(cs.vet_data__firstname||cs.vet_data__lastname),' |\.|-|','','g'),'') as key_vet,
		nullif(regexp_replace(cs.vet_data__phone,' |\.|-|\(|\)','','g'),'') 	as key_phone,
		nullif(regexp_replace(cs.vet_data__sin,'[^0-9]+', '', 'g'),'') 			as key_sln,
		nullif(lower(cs.vet_data__email),'') 									as email, 
		nullif(regexp_replace(cs.vet_data__fax ,' |\.|-|\(|\)','','g'),'') 		as fax, 
		initcap(cs.vet_data__firstname) 		as firstname,
		cs.vet_data__id 						as doctor_id,
		initcap(cs.vet_data__lastname) 			as lastname, 
		btrim(coalesce(initcap(cs.vet_data__firstname),'') || ' ' || coalesce(initcap(cs.vet_data__lastname),'')) 	as vet,
		nullif(regexp_replace(cs.vet_data__phone,' |\.|-|\(|\)','','g'),'') as phone, 
		nullif(cs.vet_data__sin,'-') 	as sln,
		ips.doctor_id 					as ips_doctor_id,
		ips.vet 						as ips_vet_name,
	    (cs.vet_data__email ilike '%ggvcp%')  as ggvcp_vet,

			
			case 
	when nullif(cs.company,'') is not null
	then cs.company
		when 
			cs.b_firstname ilike '%corpo%' 	or
			cs.b_firstname ilike '%vet%' 	or
			cs.b_firstname ilike '%hosp%' 	or
			cs.b_firstname ilike '%medic%' 	or
			cs.b_firstname ilike '%center%' or
			cs.b_firstname ilike '%animal%' or
			cs.b_firstname ilike '%pet %' 	or
			cs.b_firstname ilike '%pets%' 	or
			cs.b_firstname ilike '%clinic%'
			then initcap(cs.b_firstname)
		when 
			cs.b_lastname  ilike '%vet%' 		or
			cs.b_lastname  ilike '%hosp%' 		or
			cs.b_lastname  ilike '%medic%' 		or
			cs.b_lastname  ilike '%pet %' 		or
			cs.b_lastname  ilike '%pets%' 		or
			cs.b_lastname  ilike '%center%'		or
			cs.b_lastname  ilike '%animal%' 	or
			cs.b_lastname  ilike '%clinic%'
		then initcap(cs.b_lastname )
		when 
			cs.s_firstname ilike '%corpo%' 		or
			cs.s_firstname ilike '%vet%' 		or
			cs.s_firstname ilike '%hosp%' 		or
			cs.s_firstname ilike '%medic%' 		or
			cs.s_firstname ilike '%center%' 	or
			cs.s_firstname ilike '%animal%' 	or
			cs.s_firstname ilike '%pet %' 		or
			cs.s_firstname ilike '%pets%' 		or
			cs.s_firstname ilike '%clinic%'
		then initcap(cs.s_firstname)
		when 
			cs.s_lastname ilike '%corpo%' 		or
			cs.s_lastname ilike '%vet%' 		or
			cs.s_lastname ilike '%hosp%' 		or
			cs.s_lastname ilike '%medic%' 		or
			cs.s_lastname ilike '%pet %' 		or
			cs.s_lastname ilike '%pets%' 		or
			cs.s_lastname ilike '%center%'		or
			cs.s_lastname ilike '%animal%' 	or
			cs.s_lastname ilike '%clinic%'
		then initcap(cs.s_lastname )
		end as company,
	
case when 
	nullif(vet_data__id,'') is null then 'B2B'
	else 'B2C'
	end as channel,
case
	when nullif(cs.company,'') is null and nullif(vet_data__id,'') is null
	then false
	else true
end doctor_resgistered

FROM cscart.orders cs

left join {{ ref('dim_vet') }} ips
on ( nullif(regexp_replace(lower(cs.vet_data__firstname||cs.vet_data__lastname),' |\.|-|','','g'),'') = ips.key_vet)
or ( nullif(regexp_replace(cs.vet_data__sin,'[^0-9]+', '', 'g'),'') = ips.key_sln)
or ( nullif(regexp_replace(cs.vet_data__phone,' |\.|-|\(|\)','','g'),'') = ips.key_phone)
or 	nullif(lower(cs.vet_data__email),'')  = ips.email

order by cs.vet_data__id,
cs.timestamp desc,ips.created_date desc
