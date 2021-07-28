{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_acct_id on {{this.schema}}.{{this.name}} (account_id)"),
		after_commit("create index  index_{{this.name}}_on_doc_id on {{this.schema}}.{{this.name}} (last_doctor_id)"),
		after_commit("create index  index_{{this.name}}_on_key_acct on {{this.schema}}.{{this.name}} (key_owner)")]
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
	    last_patient_name
	FROM {{ ref('cs_der_owner') }} cs
	left join {{ ref('dim_owner') }} ips on	
		(cs.key_owner=ips.key_owner 
		or
		cs.lastname||'%' like ips.key_owner )
	and 
	(
		ips.contact_phone_numbers @> cs.array_phone
		or
		ips.contact_emails @> cs.array_email
	)