{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_acct_id on {{this.schema}}.{{this.name}} (account_id)"),
		after_commit("create index  index_{{this.name}}_on_phone on {{this.schema}}.{{this.name}} using gin (array_phone)"),
		after_commit("create index  index_{{this.name}}_on_lastname on {{this.schema}}.{{this.name}}  (lastname)")]
  })
}}
SELECT  distinct on(coalesce(nullif(cs.pet_data__user_id,'0'),nullif(cs.user_id,'0'),'U'||order_id ) )
		nullif(regexp_replace(lower(coalesce(cs.firstname,'')||coalesce(cs.lastname,'')),'\`| |\,|\&|\.|-|','','g'),'') as key_owner,
		(coalesce(nullif(cs.vet_data__id,''),'U'||cs.order_id) )	as last_doctor_id,
		btrim(initcap(coalesce(cs.b_firstname,''))||' '|| initcap(nullif(cs.b_lastname,''))) as owner_name,
		coalesce(nullif(cs.pet_data__user_id,'0'),nullif(cs.user_id,'0'),'U'||order_id )	as account_id,
		case 
			when nullif(cs.b_firstname,'') is null
			then initcap(split_part(cs.b_lastname,' ',1))
			else btrim(initcap(nullif(cs.b_firstname,'')) )	
		end as firstname,
		btrim(lower(reverse(split_part(reverse(cs.b_lastname),' ',1))))  as lastname,
		upper(cs.b_state) 										as state,
		cs.b_zipcode 											as zip,
		nullif(regexp_replace(cs.b_phone,' |\.|-|\(|\)','','g'),'')  as phone,
		nullif(cs.b_address,'') 								as address,
		nullif(cs.b_address_2,'') 								as address2,
		upper(nullif(cs.b_country,'')) 							as country,
		initcap(nullif(cs.b_county,''))							as county,
		nullif(initcap(cs.b_city),'') 							as city,
		case 
			when cs.email ilike '%ggvcp%' then null
			when cs.email ilike '%ggcvp%' then null
			else btrim(nullif(lower(cs.email),''))
		end														as email,
		nullif(regexp_replace(cs.fax,' |\.|-|\(|\)','','g'),'')	as fax,
		TIMESTAMP 'epoch' + timestamp::numeric * INTERVAL '1 second' as last_order_date,
		u.created_date,
		array_remove(array_append(array[null::text], nullif(regexp_replace(cs.b_phone,' |\.|-|\(|\)','','g'),'')),null) as array_phone,
		array_remove(array_append(array[null::text], nullif(lower(
			case 
			when cs.email ilike '%ggvcp%' then null
			when cs.email ilike '%ggcvp%' then null
			else btrim(nullif(lower(cs.email),''))
			end	
		),'')),null) as array_email,
        initcap(reverse(split_part(reverse(cs.lastname),' ',1))) || ' '||initcap(nullif(cs.pet_data__name,'')) 	as last_patient_name
FROM cscart.orders cs
left join analytics_cscart.cs_dim_user u on u.user_id=(coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id) )

order by account_id,
cs.timestamp desc
	