{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_p_id on {{this.schema}}.{{this.name}} (account_id)"),
		after_commit("create index  index_{{this.name}}_on_v_id on {{this.schema}}.{{this.name}} (doctor_id)")]
  })
}}
SELECT  distinct on(coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id) )
		'cs' 													as sys,
		nullif(regexp_replace(lower(coalesce(cs.firstname,'')||coalesce(cs.lastname,'')),' ','','g'),'') as key_account,
		nullif(regexp_replace(cs.b_phone,' |-|\(|\)','','g'),'') 	as key_phone,
		nullif(cs.vet_data__id,'0') 							as doctor_id,
		initcap(coalesce(cs.b_firstname,''))||' '|| initcap(nullif(cs.b_lastname,'')) as owner_name,
		coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id) 	as account_id,
		initcap(nullif(cs.b_firstname,'')) 						as firstname,
		initcap(nullif(cs.b_lastname,'')) 						as lastname,
		cs.b_state 												as state,
		cs.b_zipcode 											as zip,
		cs.b_phone 												as phone,
		nullif(cs.b_address,'') 								as address,
		nullif(cs.b_address_2,'') 								as address2,
		nullif(cs.b_country,'') 								as country,
		nullif(cs.b_county,'') 									as county,
		lower(cs.email) 										as email,
		nullif(cs.fax,'') 										as fax,
		cs.lang_code,
		TIMESTAMP 'epoch' + timestamp::numeric * INTERVAL '1 second' as last_order_date,
		ips.account_id 		as ips_account_id,
		ips.owner_name 		as ips_owner_name
FROM cscart.orders cs
left join {{ ref('dim_owner') }} ips on

regexp_replace(lower(coalesce(cs.firstname,'')||coalesce(lower(cs.lastname),'')),' ','','g')=ips.key_owner
or regexp_replace(lower(coalesce(cs.lastname,'')||coalesce(lower(cs.firstname),'')),' ','','g')=ips.key_owner
or ((nullif(regexp_replace(cs.b_phone,' |-|\(|\)','','g'),'')=ips.key_phone) and nullif(cs.b_phone,'') is not null)
or (cs.email=ips.email and cs.email is not null)


order by coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id) ,
timestamp desc