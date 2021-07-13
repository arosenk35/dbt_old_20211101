{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (facility_id)")]
  })
  }}
 SELECT
        pm.srno as facility_id, 
        phone11||'-'||phone12||'-'||phone13 as phone1,
        phone21||'-'||phone22||'-'||phone23 as phone2,
        fax1||'-'||fax2||'-'||fax3 as fax1,
        name, 
        address,
        note,
        z.zipid::text as zip,
        lower(pm.email) as email, 
        active,
        created_date, 
        address2,
        coalesce(z.country,'USA') as country,
        coalesce(z.state,'CA')::text as state,
        z.city 
	FROM ips.facility_master pm
  left join ips.zip_master z on pm.zip = z.srno