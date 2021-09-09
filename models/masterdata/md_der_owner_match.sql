with c1 as (
	SELECT  distinct on(cs.account_id)
		cs.key_owner,
		cs.last_doctor_id,
		cs.owner_name,
		cs.account_id,
		cs.firstname,
	    lower(cs.lastname) as lower_lastname,
		initcap(cs.lastname) as lastname,
		cs.state,
		cs.zip,
		cs.phone,
		cs.address,
		cs.address2,
		cs.country,
		cs.county,
		cs.city,
		cs.email,
		cs.fax,
		cs.last_order_date,
		ips.account_id as ips_account_id,
        ips.owner_name as ips_owner_name,
        cs.array_phone,
        cs.array_email,
		case 
			when not ips.active 
			then 99
			when cs.key_owner=ips.key_owner 
			then 1
			when ips.key_owner like '%'||cs.lastname||'%'
			then 2
			when ips.patient_name ilike '%'||cs.lastname||'%'
			then 3
			else 88
		end  as rank
	FROM {{ ref('cs_dim_owner') }} cs
	left join {{ ref('dim_owner') }} ips on	
	
			cs.key_owner=ips.key_owner 
		
	and 
		(
			ips.contact_phone_numbers @> cs.array_phone or
			ips.contact_emails @> cs.array_email
		)
	
	order by cs.account_id,rank,ips.created_date desc
	),
	
c2 as (
	
		SELECT  distinct on(cs.account_id)
		cs.key_owner,
		cs.last_doctor_id,
		cs.owner_name,
		cs.account_id,
		cs.firstname,
	    lower_lastname,
		initcap(cs.lastname) as lastname,
		cs.state,
		cs.zip,
		cs.phone,
		cs.address,
		cs.address2,
		cs.country,
		cs.county,
		cs.city,
		cs.email,
		cs.fax,
		cs.last_order_date,
		ips.account_id as ips_account_id,
		ips.owner_name as ips_owner_name,
        cs.array_phone,
        cs.array_email,
		case 
			when not ips.active 
			then 99
			when cs.key_owner=ips.key_owner 
			then 1
			when ips.key_owner like '%'||lower_lastname||'%'
			then 2
			when ips.patient_name like '%'||cs.lastname||'%'
			then 3
			else 88
		end  as rank
	FROM c1 as cs
	join {{ ref('dim_owner') }}  ips on	
	
		ips.key_owner like '%'||lower_lastname||'%'
		
	and 
		(
			ips.contact_phone_numbers @> cs.array_phone or
			ips.contact_emails @> cs.array_email
		)
	where ips_account_id is null
	order by cs.account_id,rank,ips.created_date desc)
	
	select * from c1
    union 
    select * from c2