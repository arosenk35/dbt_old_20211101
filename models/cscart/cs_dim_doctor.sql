SELECT   distinct on (doctor_data__user_id)

		nullif(doctor_data__user_id,'') 	as doctor_id,
		
		case 
		when 
			nullif(doctor_data__company,'') is not null
		then initcap(doctor_data__company)
		when 
			doctor_data__b_firstname ilike '%vet%' 		or
			doctor_data__b_firstname ilike '%hosp%' 	or
			doctor_data__b_firstname ilike '%medic%' 	or
			doctor_data__b_firstname ilike '%center%' 	or
			doctor_data__b_firstname ilike '%animal%' 	or
			doctor_data__b_firstname ilike '%pet %' 	or
			doctor_data__b_firstname ilike '%pets%' 	or
			doctor_data__b_firstname ilike '%clinic%'
		then initcap(doctor_data__b_firstname)
		when 
			doctor_data__b_lastname ilike '%vet%' 		or
			doctor_data__b_lastname ilike '%hosp%' 		or
			doctor_data__b_lastname ilike '%medic%' 	or
			doctor_data__b_lastname ilike '%pet %' 		or
			doctor_data__b_lastname ilike '%pets%' 		or
			doctor_data__b_lastname ilike '%center%'	or
			doctor_data__b_lastname ilike '%animal%' 	or
			doctor_data__b_lastname ilike '%clinic%'
		then initcap(doctor_data__b_lastname)
		end as company,
		
		initcap(nullif(doctor_data__firstname,'')) 		as firstname,
		initcap(nullif(doctor_data__lastname,'')) 		as lastname,
		
		nullif(doctor_data__b_address,'') 		as address,
		
		lower(nullif(doctor_data__email,'')) 	as email,
		coalesce(nullif(doctor_data__phone,''),nullif(doctor_data__b_phone,''),nullif(doctor_data__s_phone,'')) as phone,
			nullif(doctor_data__b_city,'') 		as city,
		nullif(doctor_data__fax,'') 			as fax,
		nullif(doctor_data__tax_exempt,'') 		as tax_excempt,
		doctor_data__b_state 					as state,
		doctor_data__b_zipcode 					as zip,
		doctor_data__b_country 					as country,
		nullif(doctor_data__birthday,'') 		as dob,
		nullif(doctor_data__url,'') 			as company_url,
		TIMESTAMP 'epoch' + timestamp::numeric * INTERVAL '1 second' as created_date,
		v.sln,
		ips_vet_name,
		ips_doctor_id
		
FROM cscart.orders cs
left join {{ ref('cs_dim_vet') }} v on v.doctor_id=cs.doctor_data__user_id
order by doctor_data__user_id,
timestamp desc