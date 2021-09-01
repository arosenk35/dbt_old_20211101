
{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (office_id)")
      ]
  })
  }}
  sELECT
        pm.srno                             as office_id, 
        phone11||phone12||phone13 as phone1,
        phone21||phone22||phone23 as phone2,
        fax1||fax2||fax3          as fax1,
        initcap(name)                       as pharmacy_name, 
        address,
        z.zipid::text                       as zip,
        lower(pm.email)                     as email, 
        coalesce(z.country,'USA')           as country,
        coalesce(z.state,'CA')::text        as state,
        initcap(z.city) as city 
FROM ips.office_master pm
  left join ips.zip_master z on pm.zip = z.srno