{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_k_p on {{this.schema}}.{{this.name}} (key_patient)"),
		after_commit("create index  index_{{this.name}}_on_k_ps on {{this.schema}}.{{this.name}} (key_patient_species)")]
  })
}}

SELECT  distinct on(   coalesce(nullif(cs.pet_data__user_id,'0'),nullif(cs.user_id,'0'),'U'||cs.order_id )|| ':' ||coalesce(cs.pet_id,'') )

        coalesce(nullif(cs.pet_data__user_id,'0'),nullif(cs.user_id,'0'),'U'||cs.order_id )|| ':' ||coalesce(cs.pet_id,'')  as patient_id,
		coalesce(cs.pet_id,'')  as cscart_patient_id,
		coalesce(nullif(cs.pet_data__user_id,'0'),cs.user_id) 	                                as account_id,
		nullif(cs.vet_data__id,'') 								                                as doctor_id,
		case 
			when 	cs.lastname ilike '%hospital%' or
					cs.lastname ilike '%clinic%' or
					cs.lastname ilike '%veten%' 
			then
				btrim(initcap(split_part(split_part(cs.email,'@',1),'.',1)) || ' '||initcap(btrim(nullif(split_part(cs.pet_data__name,' ',1),''))))
			else
				btrim(initcap(reverse(split_part(reverse(cs.lastname),' ',1))) || ' '||initcap(btrim(nullif(split_part(cs.pet_data__name,' ',1),''))))	
		end patient_name,
		initcap(btrim(nullif(split_part(cs.pet_data__name,' ',1),'')))					        as firstname,
		initcap(btrim(lower(reverse(split_part(reverse(cs.lastname),' ',1)))))                  as lastname,
		split_part(lastname,'-',1) 																as alt_lastname1,
		split_part(lastname,'-',2) 																as alt_lastname2,
		split_part(split_part(cs.email,'@',1),'.',1) 											as email_lastname,
		case 
			-- mm/dd/yyyy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{2}/\d{2}/\d{4}$'
				and split_part(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g'),'/',1)<='12'
				and regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ~  '^[0-1][0-9]/[0-2][0-9]|[3][0-1]/\d{4}$'
			then to_date(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ,'mm/dd/yyyy')
			--m/dd/yyyy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{1}/\d{2}/\d{4}$'
				and split_part(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g'),'/',1)<='9'
				and regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ~  '^[0-9]/[0-2][0-9]|[3][0-1]/\d{4}$'
			then to_date(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ,'m/dd/yyyy')
			--m/d/yyyy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{1}/\d{1}/\d{4}$'
				and split_part(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g'),'/',1)<='9'
				and regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ~ '^([0-9]/[0-9])/\d{4}$'
			then to_date(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ,'m/dd/yyyy')
			--mm/dd/yy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{2}/\d{2}/\d{2}$'
				and split_part(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g'),'/',1)<='12'
				and regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ~  '^[0-1][0-9]/[0-2][0-9]|[3][0-1]/\d{2}$'
			then to_date(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ,'mm/dd/yy')
			--mm/d/yy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{2}/\d{1}/\d{2}$'
				and split_part(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g'),'/',1)<='12'
				and regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ~  '^[0-1][0-9]/[0-9]/\d{2}$'
			then to_date(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ,'mm/dd/yy')
			--mm/d/yyyy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{2}/\d{1}/\d{4}$'
				and split_part(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g'),'/',1)<='12'
				and regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ~  '^[0-1][0-9]/[0-9]/\d{4}$'
			then to_date(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ,'mm/dd/yyyy')
			--m/dd/yy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{1}/\d{2}/\d{2}$'
				and split_part(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g'),'/',1)<='9'
				and regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ~  '^[0-9]/[0-2][0-9]|[3][0-1]/\d{2}$'
			then to_date(regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g') ,'mm/dd/yy')
			--m/d/yy
			when 
				regexp_replace(cs.pet_data__dob,'\ |\.|\-','/','g')  ~ '^\d{1}/\d{1}/\d{2}$'
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
          		when cs.pet_data__sex ilike '%male%'     	then  'Male'
          		when cs.pet_data__sex ilike '%other%'   	then  'Other'
				when cs.pet_data__sex ilike 'F%'  			then  'Female'
          		when cs.pet_data__sex ilike 'M%'    		then  'Male'
          		when cs.pet_data__sex ilike 'O%'   			then  'Other'
    	end as sex,
        case 
            when    cs.pet_data__species ilike '%dog%' or
                    cs.pet_data__species ilike '%k%9%' or
                    cs.pet_data__species ilike '%king%' or
                    cs.pet_data__species ilike '%terrier%' or
                    cs.pet_data__species ilike '%poodle%' or
                    cs.pet_data__species ilike 'cani%' or
                    cs.pet_data__species ilike '%hound%' or
                    cs.pet_data__species ilike '%malte%' or
                    cs.pet_data__species ilike '%picher%' or
                    cs.pet_data__species ilike '%doodle%' or
                    cs.pet_data__species ilike '%spaniel%' or
                    cs.pet_data__species ilike '%c%nine%' or
                    cs.pet_data__species ilike '%Ch%hua%'
            then 'dog'
            when    cs.pet_data__species ilike '%bird%' or
                    cs.pet_data__species ilike '%parrot%' or
                    cs.pet_data__species ilike '%macaw%' or
                    cs.pet_data__species ilike '%chicken%' or
                    cs.pet_data__species ilike '%pigeon%' 
            then 'bird'
            when    cs.pet_data__species ilike '%rabbit%' or 
                    cs.pet_data__species ilike '%bunny%'
            then 'rabbit'
            when    cs.pet_data__species ilike '%rat%' or
                    cs.pet_data__species ilike '%mouse%' or
                    cs.pet_data__species ilike '%rodent%'
            then 'rat'
            when    cs.pet_data__species ilike '%reptile%' or
                    cs.pet_data__species ilike '%lizard%' or
                    cs.pet_data__species ilike '%frog%' or
                    cs.pet_data__species ilike '%snake%' or
                    cs.pet_data__species ilike '%dragon%' or
                    cs.pet_data__species ilike '%chameleon%'
            then 'reptile'
            when    cs.pet_data__species ilike '%horse%'
            then 'horse'
            when    cs.pet_data__species ilike '%ferret%'
            then 'ferret'
            when    cs.pet_data__species ilike '%gerbil%' or
                    cs.pet_data__species ilike '%hamster%'
            then 'gerbil'
            when    cs.pet_data__species ilike '%cat%' or
                    cs.pet_data__species ilike '%siam%' or
                    cs.pet_data__species ilike '%tabby%' or
                    cs.pet_data__species ilike '%fe%ne%'  
            then 'cat'
            when    cs.pet_data__species ilike '%primate%' or 
                    cs.pet_data__species ilike '%monke%'
            then 'primate'
            when    cs.pet_data__species ilike '%g%pig%' or 
                    cs.pet_data__species ilike '%inea%pig%' 
            then 'guinea pig'
            else lower(btrim(regexp_replace(cs.pet_data__species,'\`|\.|-','','g'))) 
        end as species,
        lower(btrim(regexp_replace(cs.pet_data__species,'\`|\.|-','','g'))) as orig_species,
        initcap(btrim(regexp_replace(cs.pet_data__breed,'\`|\.|-','','g'))) as breed,
		nullif(lower(cs.pet_data__weight),'-')	 		                    as weight,
		TIMESTAMP 'epoch' + timestamp::numeric * INTERVAL '1 second'        as last_order_date,
		lower(regexp_replace(reverse(split_part(reverse(cs.lastname),' ',1))||split_part(cs.pet_data__name,' ',1),'\`| |\,|\&|\.|-|','','g'))  as key_patient,
		lower(regexp_replace(split_part(lastname,'-',1) ||split_part(cs.pet_data__name,' ',1),'\`| |\,|\&|\.|-|','','g'))  as key_patient1,
		lower(regexp_replace(coalesce(nullif(split_part(lastname,'-',2),''),split_part(lastname,'-',1)) ||split_part(cs.pet_data__name,' ',1),'\`| |\,|\&|\.|-|','','g'))  as key_patient2,
		lower(regexp_replace(split_part(split_part(cs.email,'@',1),'.',1)  ||split_part(cs.pet_data__name,' ',1),'\`| |\,|\&|\.|-|','','g'))  as key_patient3,
		lower(regexp_replace(cs.pet_data__name||reverse(split_part(reverse(cs.lastname),' ',1)),'\`| |\,|\&|\.|-|','','g'))  as key_patient_reverse,
        lower(regexp_replace(reverse(split_part(reverse(cs.lastname),' ',1))||cs.pet_data__name||cs.pet_data__species,'\`| |\,|\&|\.|-|','','g')) as key_patient_species

FROM cscart.orders cs
    where nullif(cs.pet_data__name,'') is not null
    order by 
        patient_id,
        cs.timestamp desc