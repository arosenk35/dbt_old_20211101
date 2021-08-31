
with c1 as(
SELECT  distinct on(cs.patient_id)
		key_patient_species,
		cs.patient_id,
	    cs.key_patient1,
	    cs.key_patient2,
		cs.key_patient3,
		cs.key_patient4,
		cs.email_lastname,
		cs.cscart_patient_id,
		cs.account_id,
		cs.doctor_id,
		cs.patient_name,
		cs.key_patient,
		cs.firstname,
		cs.lastname,
		cs.dob,
		cs.sex,
		cs.breed,
		initcap(cs.species) as species,
		orig_species,
		cs.weight,
		cs.last_order_date,
		o.ips_account_id 	as ips_owner_account_id,
		dmp.patient_id 		as ips_patient_id,
		dmp.account_id 		as ips_account_id,
		dmp.doctor_id 		as ips_doctor_id,
		case 
			when not dmp.active 
			then 99 
			when (
					dmp.key_patient=cs.key_patient||cs.species
						and o.ips_account_id=dmp.account_id
					)
					then 1
			when (
					dmp.key_patient=cs.key_patient_species
						and o.ips_account_id=dmp.account_id
					)
					then 2
			when (
				dmp.key_patient_cleaned = cs.key_patient
				and o.ips_account_id=dmp.account_id 
					)
					then 3
			when 	(
				dmp.key_patient_cleaned like cs.key_patient||'%'
				and o.ips_account_id=dmp.account_id 
					)
					then 4
			when (
					dmp.key_patient like cs.key_patient||'%'
					and o.ips_account_id=dmp.account_id 
					)
					then 5
			when (
					dmp.key_patient = cs.key_patient
					)
					then 6
			when 	(
					dmp.key_patient = cs.key_patient_reverse
					)
					then 7
			when (
					dmp.key_patient like cs.key_patient||'%'
					)
					then 10			
			when dmp.alt_key_patient = cs.key_patient
					then 11			
			when dmp.key_patient = cs.key_patient1
					then 12
			when dmp.key_patient = cs.key_patient2
					then 13

			when dmp.key_patient like '%'||cs.lastname||'%' 
					then 77
			else 88 
		end as rank
		
FROM {{ ref('cs_der_patient') }}  cs
left join {{ ref('cs_dim_owner') }}  o on o.account_id=cs.account_id
left join {{ ref('dim_patient') }}  dmp 
		on 	(
				dmp.key_patient=cs.key_patient||cs.species
					and o.ips_account_id=dmp.account_id
			)
			or 
			(
				dmp.key_patient=cs.key_patient_species
					and o.ips_account_id=dmp.account_id
			)
			or 
			(
				dmp.key_patient like cs.key_patient||'%'
				and o.ips_account_id=dmp.account_id 
			)
			or 
			(
				dmp.key_patient_cleaned = cs.key_patient
				and o.ips_account_id=dmp.account_id 
			)
			or 
			(
				dmp.key_patient_cleaned like cs.key_patient||'%'
				and o.ips_account_id=dmp.account_id 
			)
			or 
			 
			(
				dmp.key_patient = cs.key_patient1
			)
			or 
			(
				dmp.key_patient = cs.key_patient2
			)
			or 
			(
				dmp.key_patient = cs.key_patient_reverse
			)
		or
			(
				dmp.alt_key_patient = cs.key_patient
			)
where cs.patient_name is not null
order by 
		cs.patient_id,
		rank asc

),
c2 as (select 
        key_patient_species,
		cs.patient_id,
	   	cs.key_patient1,
	    cs.key_patient2,
		cs.key_patient3,
		cs.key_patient4,
		cs.email_lastname,
		cs.cscart_patient_id,
		cs.account_id,
		cs.doctor_id,
		cs.patient_name,
		cs.key_patient,
		cs.firstname,
		cs.lastname,
		cs.dob,
		cs.sex,
		cs.breed,
		cs.species,
		cs.orig_species,
		cs.weight,
		cs.last_order_date,
		cs.ips_owner_account_id,
		dmp.patient_id 		as ips_patient_id,
		dmp.account_id 		as ips_account_id,
		dmp.doctor_id 		as ips_doctor_id,
		case 
			when not dmp.active 
			then 99 
			when dmp.key_patient like cs.key_patient||'%'
			        then 20
			when dmp.key_patient like cs.key_patient1||'%'
					then 21
			when dmp.key_patient like cs.key_patient2||'%'
					then 22
			when dmp.key_patient like cs.key_patient3||'%'
					then 23
			when dmp.key_patient like cs.key_patient4||'%'
					then 24

			else cs.rank 
		end as rank


 from c1 as cs

join {{ ref('dim_patient') }}  dmp on
			(
				dmp.key_patient like cs.key_patient||'%'
			)
			or
			(
				dmp.key_patient like cs.key_patient1||'%'
			)
			or 
			(
				dmp.key_patient like cs.key_patient2||'%'
			)
			or 
			(
				dmp.key_patient like cs.key_patient3||'%'
			)
			or 
			(
				dmp.key_patient like cs.key_patient4||'%'
			)

where 	 ips_patient_id is null and nullif(cs.lastname,'') is not null )

select * from c1
union all 
select * from c2