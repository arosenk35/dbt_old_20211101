{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_acct_id on {{this.schema}}.{{this.name}} (account_id)"),
		after_commit("create index  index_{{this.name}}_on_email on {{this.schema}}.{{this.name}} (email)"),
		after_commit("create index  index_{{this.name}}_on_key_acct on {{this.schema}}.{{this.name}} using  gist (key_owner)")]
  })
}}
	SELECT  distinct on(cs.account_id)
		cs.key_owner,
		cs.last_doctor_id,
		cs.owner_name,
		cs.account_id,
		cs.firstname,
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
	FROM {{ ref('cs_der_owner') }} cs
	left join {{ ref('dim_owner') }} ips on	
		(
			cs.key_owner=ips.key_owner or
			ips.key_owner like '%'||cs.lastname||'%' or
			ips.patient_name ilike '%'||cs.lastname||'%'
		) 
	and 
		(
			ips.contact_phone_numbers @> cs.array_phone or
			ips.contact_emails @> cs.array_email
		)
	
	order by cs.account_id,rank,ips.created_date desc