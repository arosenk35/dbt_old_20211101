{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (doctor_id)"),
      after_commit("create index  index_{{this.name}}_on_clinic on {{this.schema}}.{{this.name}} (clinic)"),
      after_commit("create index  index_{{this.name}}_on_practice on {{this.schema}}.{{this.name}} (practice_id)"),
      after_commit("create index  index_{{this.name}}_on_vet on {{this.schema}}.{{this.name}} (vet)"),
      after_commit("create index  index_{{this.name}}_on_kv on {{this.schema}}.{{this.name}} (key_vet)"),
      after_commit("create index  index_{{this.name}}_on_ks on {{this.schema}}.{{this.name}} (key_sln)"),
      after_commit("create index  index_{{this.name}}_on_kclinic on {{this.schema}}.{{this.name}} (key_clinic)"),
      after_commit("create index  index_{{this.name}}_on_kp1 on {{this.schema}}.{{this.name}} (phone1)"),
      after_commit("create index  index_{{this.name}}_on_kp2 on {{this.schema}}.{{this.name}} (phone2)"),
      after_commit("create index  index_{{this.name}}_on_kp3 on {{this.schema}}.{{this.name}} (phone3)"),
      after_commit("create index  index_{{this.name}}_on_email on {{this.schema}}.{{this.name}} (email)")
      ]
  })
  }}

 SELECT distinct on( dm.srno)
    dm.srno as doctor_id, 
    initcap(firstname)  as firstname,
    initcap(lastname)   as lastname,
    nullif(address,'')  as address, 
    p.practice_id,
    {{ email_cleaned('dm.email') }} as email,
    z.zipid::text         as zip,
    nullif(btrim(phone11 || phone12|| phone13),'') as phone1,
    nullif(btrim(phone21 || phone22|| phone23),'') as phone2,
    nullif(btrim(phone31 || phone32|| phone33),'') as phone3,
    btrim(coalesce(  case 
      when  lower(credential) in ('dr','dvm') 
          then  initcap(credential)
    end,'') ||' '|| coalesce(initcap(firstname),'') || ' ' || coalesce(initcap(lastname),'')) as vet,
    created_date, 
    changed_date, 
    nullif(upin,'') as upin,
    nullif(dea,'') as dea, 
    nullif(sln,'') as sln,
    note,
    designation_id, 
    case 
      when  lower(credential) in ('dr','dvm') 
      then  initcap(credential)
    end as credential, 
    nullif(address2,'') as address2,
    coalesce(z.country,'USA') as country,
    coalesce(z.state,'CA')    as state,
	  initcap(z.city)           as city,
    case  when active='Y' 
              then true 
              else false 
          end active,

    case  when 
              address ilike '%vet%'   
          or address ilike '%hosp%' 
          or address ilike '%clinic%'
          or address ilike '%animal%'
          or address ilike '%corpor%'
          or address ilike '%pets%'
    then 
        {{gen_fuzzy_key("address")}}  
    end as key_clinic,

    case  when dm.note ilike '%vet%'    then initcap(dm.note) 
          when dm.note ilike '%hosp%'   then initcap(dm.note) 
          when dm.note ilike '%clinic%' then initcap(dm.note) 
          when dm.note ilike '%animal%' then initcap(dm.note) 
          when dm.note ilike '%corpor%' then initcap(dm.note) 
          when dm.note ilike '%pets%'   then initcap(dm.note) 
          when dm.note ilike '%medical%'  then initcap(dm.note) 
          when dm.note ilike '%dentistry%' then initcap(dm.note) 

          when address ilike '%vet%'    then initcap(address) 
          when address ilike '%medical%'  then initcap(address) 
          when address ilike '%dentistry%' then initcap(address) 
          when address ilike '%hosp%'   then initcap(address) 
          when address ilike '%clinic%' then initcap(address) 
          when address ilike '%animal%' then initcap(address) 
          when address ilike '%corpor%' then initcap(address) 
          when address ilike '%pets%'   then initcap(address) 
    end as clinic,

    nullif(regexp_replace(lower(coalesce(firstname,'')||coalesce(lastname,'')),'\`|(dvm)|(dr )|(dr.)| |\`|\,|\&|\.|-|','','g'),'') as key_vet,
		case 
        when length(nullif(regexp_replace(sln,'[^0-9]+', '', 'g'),'') ) <4 then null
        else nullif(regexp_replace(sln,'[^0-9]+', '', 'g'),'')  
    end   as key_sln,
    st.territory,
    dm.website,
    dm.dea_expiration_date,
    dm.sln_expiration_date

	FROM ips.doctor_master dm
	where  exists (select 'x' from ips.prescription p where dm.srno=p.doctor_id and p.office_id=2)
  left join ips.zip_master z on dm.zip = z.srno
  left join {{ ref('dim_practice_map') }} p on p.doctor_id=dm.srno
  left join {{ ref('sales_territories') }} st on z.zipid= st.zip
  --ignore office_id it lies!
  --where dm.office_id is null or dm.office_id =2