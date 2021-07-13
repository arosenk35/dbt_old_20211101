{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (account_id)"),
      after_commit("create index  index_{{this.name}}_on_owner on {{this.schema}}.{{this.name}} (owner_name)"),
      after_commit("create index  index_{{this.name}}_on_k_owner on {{this.schema}}.{{this.name}} (key_owner)"),
      after_commit("create index  index_{{this.name}}_on_k_phone on {{this.schema}}.{{this.name}} (key_phone)"),
      after_commit("create index  index_{{this.name}}_on_email on {{this.schema}}.{{this.name}} (email)")
      ]
  })
  }}

  SELECT distinct on (pm.srno)
        pm.srno as account_id, 
        pm.phone11 || '-' || pm.phone12 || '-' || pm.phone13  as phone1,
        pm.phone21 || '-' || pm.phone22 || '-' || pm.phone23  as phone2,
        pm.fax1 || '-' || pm.fax2 || '-' || pm.fax3           as fax1,
        initcap(pm.name )                                     as owner_name, 
        pm.address,
        pm.note,
        z.zipid::text                                         as zip,
        lower(pm.email)                                       as email, 
        pm.created_date,
        pm.address2,
        coalesce(z.country,'USA')                             as country,
        coalesce(z.state,'CA')                                as state,
        z.city,
        lower(regexp_replace(pm.name,' |\,|','','g'))         as key_owner,
		    pm.phone11 || pm.phone12 || pm.phone13                as key_phone

	FROM ips.responsible_party_master pm
  join ips.prescription p     on p.account_id=pm.srno
  left join ips.zip_master z  on pm.zip = z.srno
  where name not like '%, +%'