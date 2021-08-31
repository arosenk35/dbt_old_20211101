{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (doctor_id)"),
      after_commit("create index  index_{{this.name}}_on_clinic on {{this.schema}}.{{this.name}} (clinic)"),
      after_commit("create index  index_{{this.name}}_on_practice on {{this.schema}}.{{this.name}} (practice)"),
      after_commit("create index  index_{{this.name}}_on_vet on {{this.schema}}.{{this.name}} (vet)"),
      after_commit("create index  index_{{this.name}}_on_kv on {{this.schema}}.{{this.name}} (key_vet)"),
      after_commit("create index  index_{{this.name}}_on_ks on {{this.schema}}.{{this.name}} (key_sln)"),
      after_commit("create index  index_{{this.name}}_on_kclinic on {{this.schema}}.{{this.name}} (key_clinic)"),
      after_commit("create index  index_{{this.name}}_on_kp1 on {{this.schema}}.{{this.name}} (key_phone1)"),
      after_commit("create index  index_{{this.name}}_on_kp2 on {{this.schema}}.{{this.name}} (key_phone2)"),
      after_commit("create index  index_{{this.name}}_on_kp3 on {{this.schema}}.{{this.name}} (key_phone3)"),
      after_commit("create index  index_{{this.name}}_on_email on {{this.schema}}.{{this.name}} (email)")
      ]
  })
  }}

 SELECT 
    dm.srno as doctor_id, 
    initcap(firstname)  as firstname,
    initcap(lastname)   as lastname,
    nullif(address,'')  as address, 
    case 
      when dm.srno < 1 
      then 'Unknown' 
      else coalesce(p.practice_group,'Unknown')
    end                   as practice,
    case 
      when dm.email ilike '%ggvcp%' 
      then null
      else lower(dm.email) 
    end      as email, 
    z.zipid::text         as zip,
    phone11 ||'-'|| phone12||'-'|| phone13 as phone1,
    phone21 ||'-'|| phone22||'-'|| phone23 as phone2,
    phone31 ||'-'|| phone32||'-'|| phone33 as phone3,
    btrim(coalesce(  case 
      when  lower(credential) in ('dr','dvm') 
          then  initcap(credential)
    end,'') ||' '|| coalesce(initcap(firstname),'') || ' ' || coalesce(initcap(lastname),'')) as vet,
    created_date, 
    changed_date, 
    upin,
    dea, 
    sln,
    note,
    designation_id, 
    case 
      when  lower(credential) in ('dr','dvm') 
      then  initcap(credential)
    end as credential, 
    address2,
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
          or address ilike '%center%'
          or address ilike '%corpor%'
          or address ilike '%pets%'
    then nullif(regexp_replace(lower(coalesce(address,'')),'\`|\(|\)| |\,|\&|\.|-|','','g'),'')
    end as key_clinic,

        case  
          when address ilike '%vet%'    then initcap(address) 
          when address ilike '%hosp%'   then initcap(address) 
          when address ilike '%clinic%' then initcap(address) 
          when address ilike '%animal%' then initcap(address) 
          when address ilike '%center%' then initcap(address) 
          when address ilike '%corpor%' then initcap(address) 
          when address ilike '%pets%'   then initcap(address) 
    end as clinic,

     nullif(regexp_replace(lower(coalesce(firstname,'')||coalesce(lastname,'')),'\`|(dvm)|(dr )|(dr.)| |\`|\,|\&|\.|-|','','g'),'') as key_vet,
		phone11 || phone12|| phone13  as key_phone1,
    phone21 || phone22|| phone23  as key_phone2,
    phone31 || phone32|| phone33  as key_phone3,
		case 
        when length(nullif(regexp_replace(sln,'[^0-9]+', '', 'g'),'') ) <4 then null
        else nullif(regexp_replace(sln,'[^0-9]+', '', 'g'),'')  
    end   as key_sln

	FROM ips.doctor_master dm
  left join ips.zip_master z on dm.zip = z.srno
  left join {{ ref('dim_practice_map') }} p on p.practice=coalesce(nullif(dm.address,''),dm.note)