{{
  config({
      "materialized": "table",
      "post-hook": [
      after_commit("create index if not exists index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (patient_id)"),
      after_commit("create index if not exists index_{{this.name}}_on_doc_id on {{this.schema}}.{{this.name}} (doctor_id)"),
      after_commit("create index if not exists index_{{this.name}}_on_acct_id on {{this.schema}}.{{this.name}} (account_id)"),
      after_commit("create index if not exists index_{{this.name}}_on_name on {{this.schema}}.{{this.name}} (patient_name)"),
      after_commit("create index if not exists index_{{this.name}}_on_zip_state on {{this.schema}}.{{this.name}} (zip,state)"),
      after_commit("create index if not exists index_{{this.name}}_on_zip_state on {{this.schema}}.{{this.name}} (key_pet)")
          ]
    })
}}

SELECT distinct on (pm.id)
    pm.id                                                as patient_id,
    pm.phone11 || '-' || pm.phone12 || '-' || pm.phone13 as phone1,
    pm.phone21 || '-' || pm.phone22 || '-' || pm.phone23 as phone2,
    pm.phone31 || '-' || pm.phone32 || '-' || pm.phone33 as phone3,
    coalesce(initcap(pm.firstname),'')                   as patient_name,
    initcap(pm.lastname)    as lastname ,
    initcap(pm.firstname)   as firstname,
    initcap(pm.middlename)  as middlename,
    pm.bdate                as dob, 
    case  when pm.sex ilike '%female%'  then  'F'
          when pm.sex ilike 'male%'     then  'M'
          when pm.sex ilike '%other%'   then  'O'
          else pm.sex
    end as sex,
    lower(pm.email)          as email, 
    pm.patnote, 
    pm.created_date, 
    pm.address,
    pm.address2,
    z.zipid::text            as zip, 
    pm.death_date, 
    initcap(pm.patient_type) as species,
    pm.deceased_date, 
    pm.account_type, 
    pm.account_id,
    pd.doctor_id, 
    pm.pregnant_flag,
    pm.office_id,
    upper(coalesce(z.country,pm.country,'USA')) as country,
    upper(coalesce(z.state,'CA'))               as state,
	initcap(z.city)                             as city,
    coalesce(lower(pm.firstname),'')            as key_pet
	FROM ips.patient_master pm
  join ips.prescription p on p.patient_id=pm.id
  left join {{ ref('dim_patient_doctor') }} pd on pm.id=pd.patient_id
  left join ips.zip_master z on pm.zip = z.srno