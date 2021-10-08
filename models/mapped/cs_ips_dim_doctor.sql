{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_d_id on {{this.schema}}.{{this.name}} (doctor_id)")]
  })
}}

SELECT   
		distinct on (cs.doctor_id)

		cs.*,
		ips.doctor_id 			as ips_doctor_id,
		ips.vet 				as ips_doctor_name,
		ips.sln    				as ips_sln,
		ips.clinic 				as ips_clinic,
		ips.practice 			as ips_practice,
		ips.dea 				as ips_dea,

	case 
		when not ips.active then 99 
		when cs.key_sln = ips.key_sln 
		and cs.key_clinic =ips.key_clinic
		then 1
		when cs.key_vet	 	= 	ips.key_vet
		and cs.key_clinic 	=	ips.key_clinic
		then 2		
		when cs.key_sln = ips.key_sln 
		and cs.zip=ips.zip
		then 5	
		when cs.key_vet	 = ips.key_vet
		and cs.zip=ips.zip
		then 6	
		when cs.key_sln = ips.key_sln 
		and regexp_replace(lower(cs.city),' ','','g')=regexp_replace(lower(ips.city),' ','','g')
		then 7	
		when cs.key_vet	 = ips.key_vet
		and regexp_replace(lower(cs.city),' ','','g')=regexp_replace(lower(ips.city),' ','','g')
		then 8
		when cs.key_sln = ips.key_sln 
		and (	phone 	=	ips.phone1
			or 	phone  	=	ips.phone2
			or 	phone  	=	ips.phone3
			or  fax 	=	ips.phone1
			or 	fax  	=	ips.phone2
			or 	fax  	=	ips.phone3)
		then 9		
		when  	cs.key_vet	 = ips.key_vet 
		and (	phone 	=	ips.phone1
			or 	phone 	=	ips.phone2
			or 	phone 	=	ips.phone3
			or  fax 	=	ips.phone1
			or 	fax  	=	ips.phone2
			or 	fax  	=	ips.phone3)
		then 10		
		when cs.key_sln = ips.key_sln 
		then 50		
		when cs.key_vet	 = ips.key_vet
		then 51		
		when cs.email  = ips.email
		then 52
		else 88
	end  as rank

FROM {{ ref('cs_dim_doctor') }} cs

left join {{ ref('dim_vet') }} ips
	on 	cs.key_vet	=	ips.key_vet
	or 	cs.key_sln	= 	ips.key_sln
	or 	cs.email  	= 	ips.email

order by 
	cs.doctor_id,
	rank asc,
	ips.created_date desc
