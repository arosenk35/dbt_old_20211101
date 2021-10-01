{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_acct_id on {{this.schema}}.{{this.name}} (account_id)"),
		after_commit("create index  index_{{this.name}}_on_arr_phone on {{this.schema}}.{{this.name}} using gin (array_phone)"),
		after_commit("create index  index_{{this.name}}_on_arr_email on {{this.schema}}.{{this.name}} using gin (array_email)"),
		after_commit("create index  index_{{this.name}}_on_lastname on {{this.schema}}.{{this.name}}  (lastname)"),
		after_commit("create index  index_{{this.name}}_on_k_owner on {{this.schema}}.{{this.name}}  (key_owner)"),
		after_commit("create index  index_{{this.name}}_on_email on {{this.schema}}.{{this.name}} (email)")]
  })
}}
SELECT  distinct on(coalesce(nullif(cs.pet_data__user_id,'0'),nullif(cs.user_id,'0'),'U'||order_id ) )
		nullif(regexp_replace(lower(coalesce(cs.firstname,'')||coalesce(cs.lastname,'')),'\`| |\,|\&|\.|-|','','g'),'') as key_owner,
		(coalesce(nullif(cs.vet_data__id,''),'U'||cs.order_id) )								as last_doctor_id,
		btrim(initcap(coalesce(cs.b_firstname,''))||' '|| initcap(nullif(cs.b_lastname,''))) 	as owner_name,
		coalesce(nullif(cs.pet_data__user_id,'0'),nullif(cs.user_id,'0'),'U'||order_id )		as account_id,
		case 
			when 	nullif(cs.b_lastname,'') ilike '%vet%'    or
					nullif(cs.b_lastname,'') ilike '%hosp%'   or
				 	nullif(cs.b_lastname,'') ilike '%clinic%' or
                 	nullif(cs.b_lastname,'') ilike '%animal%' or
                 	nullif(cs.b_lastname,'') ilike '%center%' or
                 	nullif(cs.b_lastname,'') ilike '%corpor%' 
			then 	initcap(split_part(cs.b_lastname,' ',1))
			when 	nullif(cs.b_firstname,'') is null
			then 	initcap(split_part(cs.b_lastname,' ',1))
			else 	btrim(initcap(nullif(cs.b_firstname,'')) )	
		end as firstname,
		case 
			when 	nullif(cs.b_lastname,'') is null
			then 	btrim(lower(reverse(split_part(reverse(cs.b_firstname),' ',1))))
			when 	nullif(cs.b_lastname,'') is not null and (
			     	nullif(cs.b_lastname,'') ilike '%vet%'    or
				 	nullif(cs.b_lastname,'') ilike '%hosp%'   or
				 	nullif(cs.b_lastname,'') ilike '%clinic%' or
                 	nullif(cs.b_lastname,'') ilike '%animal%' or
                 	nullif(cs.b_lastname,'') ilike '%center%' or
                 	nullif(cs.b_lastname,'') ilike '%corpor%' )
			then btrim(initcap(substring(cs.b_lastname,POSITION(' ' in cs.b_lastname)+1,40)))
   			else btrim(initcap(reverse(split_part(reverse(btrim(cs.b_lastname)),' ',1))))  
		end as lastname,
		upper(btrim(cs.b_state)) 										as state,
		cs.b_zipcode 											as zip,
		nullif(regexp_replace(cs.b_phone,' |\.|-|\(|\)','','g'),'')  as phone,
		btrim(nullif(cs.b_address,'')) 							as address,
		btrim(nullif(cs.b_address_2,''))						as address2,
		upper(nullif(cs.b_country,'')) 							as country,
		initcap(nullif(cs.b_county,''))							as county,
		initcap(btrim(nullif(cs.b_city,'')))							as city,
		case 
			when cs.email ilike '%ggvcp%' then null
			when cs.email ilike '%ggcvp%' then null
			else btrim(nullif(lower(cs.email),''))
		end																as email,
		nullif(regexp_replace(cs.fax,' |\.|-|\(|\)','','g'),'')			as fax,
		TIMESTAMP 'epoch' + timestamp::numeric * INTERVAL '1 second' 	as last_order_date,
		u.created_date,
		array_remove(array_append(array[null::text], nullif(regexp_replace(cs.b_phone,' |\.|-|\(|\)','','g'),'')),null) as array_phone,
		array_remove(array_append(array[null::text], nullif(lower(
			case 
			when cs.email ilike '%ggvcp%' then null
			when cs.email ilike '%ggcvp%' then null
			else btrim(nullif(lower(cs.email),''))
			end	
		),'')),null) as array_email,
        initcap(reverse(split_part(reverse(cs.lastname),' ',1))) || ' '||initcap(nullif(cs.pet_data__name,'')) 	as last_patient_name,
        case 	when coalesce(cs.b_firstname,'')||nullif(cs.b_lastname,'') ilike '%banfield%'    	then 'Corp'
		        when coalesce(cs.b_firstname,'')||nullif(cs.b_lastname,'') ilike '%vca%'    		then 'Corp'
				when coalesce(cs.b_firstname,'')||nullif(cs.b_lastname,'') ilike '%petco%'    		then 'Corp'
				when coalesce(cs.b_firstname,'')||nullif(cs.b_lastname,'') ilike '%village%'    	then 'Corp'
				when coalesce(cs.b_firstname,'')||nullif(cs.b_lastname,'') ilike '%vet%'    then 'Clinic'
				when coalesce(cs.b_firstname,'')||nullif(cs.b_lastname,'') ilike '%hosp%'   then 'Clinic'
				when coalesce(cs.b_firstname,'')||nullif(cs.b_lastname,'') ilike '%clinic%' then 'Clinic'
                when coalesce(cs.b_firstname,'')||nullif(cs.b_lastname,'') ilike '%animal%' then 'Clinic'
                when coalesce(cs.b_firstname,'')||nullif(cs.b_lastname,'') ilike '%center%' then 'Clinic'
                when coalesce(cs.b_firstname,'')||nullif(cs.b_lastname,'') ilike '%corpor%' then 'Clinic'
                when cs.email ilike '%vet%'     then 'Clinic'
                when cs.email ilike '%hosp%'    then 'Clinic'
                when cs.email ilike '%clinic%'  then 'Clinic'
                when cs.email ilike '%animal%'  then 'Clinic'
                when cs.email ilike '%center%'  then 'Clinic'
                when cs.email ilike '%corpo%'   then 'Clinic'
                when cs.email ilike '%payab%'   then 'Clinic'
                when cs.email ilike '%account%' then 'Clinic'
                else 'Patient'
        end as account_type

FROM cscart.orders cs
left join {{ ref('cs_dim_user') }} u on u.user_id=(coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id) )

order by account_id,
cs.timestamp desc
	