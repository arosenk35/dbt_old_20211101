{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (account_id)"),
      after_commit("create index  index_{{this.name}}_on_k_owner on {{this.schema}}.{{this.name}} USING gin (key_owner gin_trgm_ops)"),
      after_commit("create index  index_{{this.name}}_on_email on {{this.schema}}.{{this.name}} (email)"),
      after_commit("create index  index_{{this.name}}_on_p_numbers on {{this.schema}}.{{this.name}} USING gin (contact_phone_numbers)"),
      after_commit("create index  index_{{this.name}}_on_emails on {{this.schema}}.{{this.name}} USING gin (contact_emails)")
      ]
  })
  }}

  SELECT distinct on (pm.srno)
        pm.srno as account_id, 
        pm.phone11 || '-' || pm.phone12 || '-' || pm.phone13  as phone1,
        pm.phone21 || '-' || pm.phone22 || '-' || pm.phone23  as phone2,
        pm.fax1 || '-' || pm.fax2 || '-' || pm.fax3           as fax,
        initcap(coalesce(nullif(pm.care_of,''),pm.name))      as owner_name, 
        initcap(nullif(pm.care_of,'')) as care_of,
        initcap(nullif(pm.name,'')) as patient_name,
        pm.address,
        pm.note,
        z.zipid::text                                         as zip,
        lower(pm.email)                                       as email, 
        pm.created_date,
        pm.address2,
        coalesce(upper(z.country),'USA')                      as country,
        coalesce(upper(z.state),'CA')                         as state,
        initcap(z.city) as city,
        lower(regexp_replace( coalesce(nullif(pm.care_of,''),pm.name) ,' |\,|\&|\.|-|','','g'))    as key_owner,
		    pm.phone11 || pm.phone12 || pm.phone13                as key_phone,
        coalesce(oc.phone_numbers,'{}') as contact_phone_numbers,
        coalesce(oc.emails,'{}') as contact_emails,
        case 		when coalesce(nullif(pm.care_of,''),pm.name) ilike '%vet%' then 'Clinic'
							  when coalesce(nullif(pm.care_of,''),pm.name) ilike '%hosp%' then 'Clinic'
							  when coalesce(nullif(pm.care_of,''),pm.name) ilike '%clinic%' then 'Clinic'
                when coalesce(nullif(pm.care_of,''),pm.name) ilike '%animal%' then 'Clinic'
                when coalesce(nullif(pm.care_of,''),pm.name) ilike '%center%' then 'Clinic'
                when coalesce(nullif(pm.care_of,''),pm.name) ilike '%corpor%' then 'Clinic'
                else 'Patient'
        end as account_type,
        case when active='Y' then true else false end active,

	FROM ips.responsible_party_master pm
  left join {{ ref('dim_owner_contacts') }} oc on pm.srno=oc.account_id
  join ips.prescription p     on p.account_id=pm.srno
  left join ips.zip_master z  on pm.zip = z.srno
  where name not like '%, +%'
  order by pm.srno,p.created_date desc