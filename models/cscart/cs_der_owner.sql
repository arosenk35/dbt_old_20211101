{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_acct_id on {{this.schema}}.{{this.name}} (account_id)")]
  })
}}
SELECT  distinct on(coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id) )
		nullif(regexp_replace(lower(coalesce(cs.firstname,'')||coalesce(cs.lastname,'')),' |\,|\&|\.|-|','','g'),'') as key_owner,
		(coalesce(nullif(cs.vet_data__id,''),'U'||cs.order_id) )	as last_doctor_id,
		btrim(initcap(coalesce(cs.b_firstname,''))||' '|| initcap(nullif(cs.b_lastname,''))) as owner_name,
		coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id) 	as account_id,
		initcap(nullif(cs.b_firstname,'')) 						as firstname,
		lower(reverse(split_part(reverse(cs.lastname),' ',1)))  as lastname,
		upper(cs.b_state) 										as state,
		cs.b_zipcode 											as zip,
		nullif(regexp_replace(cs.b_phone,' |\.|-|\(|\)','','g'),'')  as phone,
		nullif(cs.b_address,'') 								as address,
		nullif(cs.b_address_2,'') 								as address2,
		upper(nullif(cs.b_country,'')) 							as country,
		initcap(nullif(cs.b_county,''))							as county,
		nullif(initcap(cs.b_city),'') 							as city,
		nullif(lower(cs.email),'') 								as email,
		nullif(regexp_replace(cs.fax,' |\.|-|\(|\)','','g'),'')	as fax,
		TIMESTAMP 'epoch' + timestamp::numeric * INTERVAL '1 second' as last_order_date,
		array_remove(array_append(array[null::text], nullif(regexp_replace(cs.b_phone,' |\.|-|\(|\)','','g'),'')),null) as array_phone,
		array_remove(array_append(array[null::text], nullif(lower(cs.email),'')),null) as array_email,
        initcap(reverse(split_part(reverse(cs.lastname),' ',1))) || ' '||initcap(nullif(cs.pet_data__name,'')) 	as last_patient_name
FROM cscart.orders cs

order by coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id) ,
cs.timestamp desc
	