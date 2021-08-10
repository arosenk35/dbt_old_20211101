{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_pat_id on {{this.schema}}.{{this.name}} (account_id)"),
		after_commit("create index  index_{{this.name}}_on_pet_id on {{this.schema}}.{{this.name}} (patient_id)")]
  })
}}
SELECT  distinct on(coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id)|| coalesce(cs.pet_id,'') )

        coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id) || ':' || coalesce(cs.pet_id,'')  as patient_id,
		coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id) 	as account_id,
		nullif(cs.vet_data__id,'') 								as doctor_id,
		initcap(reverse(split_part(reverse(cs.lastname),' ',1))) || ' '||initcap(nullif(cs.pet_data__name,'')) 	as patient_name,
		initcap(nullif(cs.pet_data__name,'')) 					as firstname,
		initcap(btrim(lower(reverse(split_part(reverse(cs.b_lastname),' ',1))))) as lastname,
		case 
			-- mm/dd/yyyy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{2}/\d{2}/\d{4}'
				and split_part(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g'),'/',1)<='12'
				and regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ~  '^[0-1][0-9]/[0-2][0-9]|[3][0-1]/\d{4}$'
			then to_date(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ,'mm/dd/yyyy')
			--m/dd/yyyy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{1}/\d{2}/\d{4}'
				and split_part(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g'),'/',1)<='9'
				and regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ~  '^[0-9]/[0-2][0-9]|[3][0-1]/\d{4}$'
			then to_date(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ,'m/dd/yyyy')
			--m/d/yyyy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{1}/\d{1}/\d{4}'
				and split_part(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g'),'/',1)<='9'
				and regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ~ '^([0-9]/[0-9])/\d{4}$'
			then to_date(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ,'m/dd/yyyy')
			--mm/dd/yy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{2}/\d{2}/\d{2}'
				and split_part(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g'),'/',1)<='12'
				and regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ~  '^[0-1][0-9]/[0-2][0-9]|[3][0-1]/\d{2}$'
			then to_date(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ,'mm/dd/yy')
			--mm/d/yy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{2}/\d{1}/\d{2}'
				and split_part(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g'),'/',1)<='12'
				and regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ~  '^[0-1][0-9]/[0-9]/\d{2}$'
			then to_date(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ,'mm/dd/yy')
			--mm/d/yyyy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{2}/\d{1}/\d{4}'
				and split_part(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g'),'/',1)<='12'
				and regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ~  '^[0-1][0-9]/[0-9]/\d{4}$'
			then to_date(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ,'mm/dd/yyyy')
			--m/dd/yy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{1}/\d{2}/\d{2}'
				and split_part(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g'),'/',1)<='9'
				and regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ~  '^[0-9]/[0-2][0-9]|[3][0-1]/\d{2}$'
			then to_date(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ,'mm/dd/yy')
			--m/d/yy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{1}/\d{1}/\d{2}'
				and split_part(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g'),'/',1)<='9'
				and regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ~ '^([0-9]/[0-9])/\d{2}$'
			then to_date(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ,'m/dd/yy')
			--yyyy
			when 
				cs.pet_data__dob ~ '^\d{4}$'
			then to_date('0101'||cs.pet_data__dob ,'mmddyyyy')
			--m/yyyy
			when 
				cs.pet_data__dob ~ '^\d{1}/\d{4}$'
			then to_date('01/0'||replace(cs.pet_data__dob,'-','/') ,'dd/mm/yyyy')
			--mm/yyyy
			when 
				cs.pet_data__dob ~ '^\d{2}/\d{4}$'
			then to_date('01/'||replace(cs.pet_data__dob,'-','/') ,'dd/mm/yyyy')
			
		end::date as dob,
		case  	when cs.pet_data__sex ilike '%female%'  	then  'Female'
          		when cs.pet_data__sex ilike 'male%'     	then  'Male'
          		when cs.pet_data__sex ilike '%other%'   	then  'Other'
    	end as sex,
		initcap(nullif(cs.pet_data__species,'-')) 		as species,
		initcap(nullif(cs.pet_data__breed,'-')) 		as breed,
		nullif(lower(cs.pet_data__weight),'-')	 		as weight,
		TIMESTAMP 'epoch' + timestamp::numeric * INTERVAL '1 second' as last_order_date,
		o.ips_account_id,
		dmp.patient_id as ips_patient_id,
		lower(regexp_replace(reverse(split_part(reverse(cs.lastname),' ',1))||cs.pet_data__name,' |\,|\&|\.|-|','','g'))  as key_pet
FROM cscart.orders cs
left join {{ ref('cs_dim_owner') }} o on o.account_id=coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id)
left join {{ ref('dim_patient') }} dmp 
 on (
(dmp.key_pet=lower(regexp_replace(reverse(split_part(reverse(cs.lastname),' ',1))||cs.pet_data__name||cs.pet_data__species,' |\,|\&|\.|-|','','g'))
and o.ips_account_id=dmp.account_id ))
or 
(dmp.key_pet like lower(regexp_replace(reverse(split_part(reverse(cs.lastname),' ',1))||cs.pet_data__name,' |\,|\&|\.|-|','','g')||'%')
	and o.ips_account_id=dmp.account_id )
where nullif(cs.pet_data__name,'') is not null
order by coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id)|| coalesce(cs.pet_id,'') ,
timestamp desc