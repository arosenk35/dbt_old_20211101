---test

{{
  config({
      "materialized": "table",
      "post-hook": [
      after_commit("create index if not exists index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (patient_id)"),
      after_commit("create index if not exists index_{{this.name}}_on_doc_id on {{this.schema}}.{{this.name}} (doctor_id)"),
      after_commit("create index if not exists index_{{this.name}}_on_acct_id on {{this.schema}}.{{this.name}} (account_id)"),
      after_commit("create index if not exists index_{{this.name}}_on_pat_name on {{this.schema}}.{{this.name}} (patient_name)"),
      after_commit("create index if not exists index_{{this.name}}_on_zip_state on {{this.schema}}.{{this.name}} (zip,state)"),
      after_commit("create index if not exists index_{{this.name}}_on_key_patient on {{this.schema}}.{{this.name}} using gist (key_patient)"),
      after_commit("create index if not exists index_{{this.name}}_on_alt_key_patient on {{this.schema}}.{{this.name}} using gist (alt_key_patient)"),
      after_commit("create index if not exists index_{{this.name}}_on_key_patient_cleaned on {{this.schema}}.{{this.name}} using gist (key_patient_cleaned)")
          ]
    })
}}

SELECT distinct on (pm.id)
    pm.id                                                as patient_id,
    nullif(btrim(pm.phone11 ||  pm.phone12 ||  pm.phone13),'') as phone1,
    nullif(btrim(pm.phone21 ||  pm.phone22 ||  pm.phone23),'') as phone2,
    nullif(btrim(pm.phone31 ||  pm.phone32 ||  pm.phone33),'') as phone3,
    initcap(split_part(split_part(firstname, ' ',1),'-',1)) ||' '|| initcap(nullif(split_part(split_part(lastname, ' ',1),'-',1),'')) as patient_name,
    initcap(nullif(split_part(split_part(lastname, ' ',1),'-',1),''))   as lastname,
	initcap(nullif(pm.lastname,''))    as orig_lastname,
    initcap(nullif(split_part(split_part(firstname, ' ',1),'-',1),''))  as firstname,
    initcap(nullif(pm.middlename,''))  as middlename,
    initcap(btrim(lower(reverse(split_part(reverse(address2),' ',1)))))   as lastname_alternative,
    pm.bdate                as dob, 
    case  when pm.sex ilike '%female%'  then  'Female'
          when pm.sex ilike 'male%'     then  'Male'
          when pm.sex ilike '%other%'   then  'Other'
          else nullif(pm.sex,'')
    end as sex,
    {{ email_cleaned('pm.email') }} as email,
    pm.patnote, 
    pm.created_date, 
    pm.address,
    pm.address2,
    z.zipid::text as zip, 
    pm.death_date, 
    initcap(pm.patient_type) as species,
    pm.deceased_date as dod, 
    pm.account_type, 
    pm.account_id,
    pd.doctor_id, 
    pm.pregnant_flag,
    pm.office_id,
    case when pm.active='Y' then true else false end active,
    upper(coalesce(z.country,pm.country,'USA')) as country,
    upper(coalesce(z.state,'CA'))               as state,
	initcap(z.city)                             as city,
    {{gen_fuzzy_key("pm.lastname||pm.firstname")}}  as key_patient,
    {{gen_fuzzy_key("reverse(split_part(reverse(address2),' ',1))||split_part(split_part(firstname, ' ',1),'-',1)")}}  as alt_key_patient,
    {{gen_fuzzy_key("split_part(split_part(lastname, ' ',1),'-',1)||split_part(split_part(firstname, ' ',1),'-',1)")}}   as key_patient_cleaned
	FROM ips.patient_master pm
  join ips.prescription p on p.patient_id=pm.id
  left join {{ ref('dim_patient_doctor') }} pd on pm.id=pd.patient_id
  left join ips.zip_master z on pm.zip = z.srno
  where  pm.office_id=2
  order by 
        pm.id , 
        p.created_date desc