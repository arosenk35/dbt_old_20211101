{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_d_id on {{this.schema}}.{{this.name}} (doctor_id)")]
  })
}}

SELECT   
		distinct on (coalesce(nullif(cs.vet_data__id,''),'U'||cs.order_id) )

		nullif(regexp_replace(lower(cs.vet_data__firstname||cs.vet_data__lastname),' |\,|\&|\.|-|','','g'),'') as key_vet,
		nullif(regexp_replace(cs.vet_data__phone,' |\.|-|\(|\)','','g'),'') 	as key_phone,
		nullif(regexp_replace(cs.vet_data__sin,'[^0-9]+', '', 'g'),'') 			as key_sln,
		nullif(lower(cs.vet_data__email),'') 									as email, 
		nullif(regexp_replace(cs.vet_data__fax ,' |\.|-|\(|\)','','g'),'') 		as fax, 
		initcap(cs.vet_data__firstname) 		as firstname,
		(coalesce(nullif(cs.vet_data__id,''),'U'||cs.order_id) )				as doctor_id,
		initcap(cs.vet_data__lastname) 			as lastname, 
		nullif(btrim(coalesce(initcap(cs.vet_data__firstname),'') || ' ' || coalesce(initcap(cs.vet_data__lastname),'')),'') as doctor_name,
		nullif(regexp_replace(cs.vet_data__sin ,' |\.|-','','g'),'') 	as sln,
		nullif(cs.doctor_data__b_address_2,'') 						as address2,
		coalesce(nullif(doctor_data__user_type,''),'U') 			as user_type,
		nullif(doctor_data__clinic_id,'')  		as clinic_id,
	    (cs.vet_data__email ilike '%ggvcp%')  	as ggvcp_vet,
		nullif(doctor_data__b_address,'') 		as address,
		coalesce(nullif(regexp_replace(cs.vet_data__phone,' |\,|\&|\.|-|\(|\)','','g'),''),nullif(regexp_replace(doctor_data__phone,' |\,|\&|\.|-|\(|\)','','g'),''),nullif(regexp_replace(doctor_data__b_phone,' |-|\(|\)','','g'),''),nullif(regexp_replace(doctor_data__s_phone,' |-|\(|\)','','g'),'')) as phone,
		nullif(initcap(doctor_data__b_city),'') as city,
		nullif(doctor_data__tax_exempt,'') 		as tax_excempt,
		upper(nullif(doctor_data__b_state,'')) 	as state,
		nullif(doctor_data__b_zipcode,'') 		as zip,
		upper(doctor_data__b_country) 			as country,
		nullif(doctor_data__birthday,'') 		as dob,
		nullif(doctor_data__url,'') 			as company_url,
		nullif(doctor_data__staff_notes,'') 	as staff_notes,
	case 
		when nullif(regexp_replace(cs.company,' |-|','','g'),'') is not null
		then nullif(regexp_replace(cs.company,' |-|','','g'),'')
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
			cs.b_lastname  ilike '%corpo%' 	or 
			cs.b_lastname  ilike '%vet%' 	or
			cs.b_lastname  ilike '%hosp%' 	or
			cs.b_lastname  ilike '%medic%' 	or
			cs.b_lastname  ilike '%pet %' 	or
			cs.b_lastname  ilike '%pets%' 	or
			cs.b_lastname  ilike '%center%'	or
			cs.b_lastname  ilike '%animal%' or
			cs.b_lastname  ilike '%clinic%'
		then initcap(cs.b_lastname )
		when 
			cs.s_firstname ilike '%corpo%' 	or
			cs.s_firstname ilike '%vet%' 	or
			cs.s_firstname ilike '%hosp%' 	or
			cs.s_firstname ilike '%medic%' 	or
			cs.s_firstname ilike '%center%' or
			cs.s_firstname ilike '%animal%' or
			cs.s_firstname ilike '%pet %' 	or
			cs.s_firstname ilike '%pets%' 	or
			cs.s_firstname ilike '%clinic%'
		then initcap(cs.s_firstname)
		when 
			cs.s_lastname ilike '%corpo%' 	or
			cs.s_lastname ilike '%vet%' 	or
			cs.s_lastname ilike '%hosp%' 	or
			cs.s_lastname ilike '%medic%' 	or
			cs.s_lastname ilike '%pet %' 	or
			cs.s_lastname ilike '%pets%' 	or
			cs.s_lastname ilike '%center%'	or
			cs.s_lastname ilike '%animal%' 	or
			cs.s_lastname ilike '%clinic%'
		then initcap(cs.s_lastname )
		end as company,
	
	case when 
		nullif(vet_data__id,'') is null and
		nullif(regexp_replace(lower(cs.vet_data__firstname||cs.vet_data__lastname),' ','','g'),'') is null
		then 'B2B' 
		else 'B2C'
		end as channel,
	case
		when nullif(regexp_replace(cs.company,' |-|','','g'),'') is null and nullif(vet_data__id,'') is null
		then false
		else true
	end doctor_resgistered,
		ips.doctor_id 										as ips_doctor_id,
		ips.vet 											as ips_doctor_name

FROM cscart.orders cs

left join {{ ref('dim_vet') }} ips
	on ( nullif(regexp_replace(lower(cs.vet_data__firstname||cs.vet_data__lastname),' |\,|\&|\.|-|','','g'),'') = ips.key_vet)
	or ( nullif(regexp_replace(cs.vet_data__sin,'[^0-9]+', '', 'g'),'') = ips.key_sln)
	or 	lower(case when cs.vet_data__email ilike '%ggvcp%' then null else cs.vet_data__email end)  = ips.email

order by (coalesce(nullif(cs.vet_data__id,''),'U'||cs.order_id) ),
	cs.timestamp desc,
	case when active then 1 else 99 end asc,
	ips.created_date desc
