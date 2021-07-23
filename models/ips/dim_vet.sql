{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}}  (doctor_id)"),
      after_commit("create index  index_{{this.name}}_on_vet on {{this.schema}}.{{this.name}} (vet)"),
      after_commit("create index  index_{{this.name}}_on_vet_k on {{this.schema}}.{{this.name}} (key_vet)"),
      after_commit("create index  index_{{this.name}}_on_vet_s on {{this.schema}}.{{this.name}} (key_sln)"),
      after_commit("create index  index_{{this.name}}_on_vet_p on {{this.schema}}.{{this.name}} (key_phone)"),
      after_commit("create index  index_{{this.name}}_on_vet_e on {{this.schema}}.{{this.name}} (email)")
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
    lower(dm.email)       as email, 
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

    nullif(regexp_replace(lower(coalesce(firstname,'')||coalesce(lastname,'')),' |\.','','g'),'') as key_vet,
		nullif(regexp_replace(phone1,' |-|\(|\)','','g'),'')  as key_phone,
		nullif(regexp_replace(sln,'[^0-9]+', '', 'g'),'')     as key_sln

	FROM ips.doctor_master dm
  left join ips.zip_master z on dm.zip = z.srno
  left join {{ ref('dim_practice_map') }} p on p.practice=coalesce(nullif(dm.address,''),dm.note)