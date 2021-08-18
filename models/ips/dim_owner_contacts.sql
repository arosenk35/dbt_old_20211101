{{
  config({
    "materialized": "table",
    "post-hook": [
        
    after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (account_id)"),
    after_commit("create index  index_{{this.name}}_on_p_numbers on {{this.schema}}.{{this.name}} USING GIN(phone_numbers)"),
    after_commit("create index  index_{{this.name}}_on_emails on {{this.schema}}.{{this.name}} USING GIN(emails)")
      ]
  })
  }}
  
  with contacts as (
    SELECT 
        pm.account_id,
        array_remove(array_agg(distinct nullif(pm.phone11,'') ||pm.phone12 ||  pm.phone13),null) as phone1,
        array_remove(array_agg(distinct nullif(pm.phone21,'') ||pm.phone22 ||  pm.phone23),null) as phone2,	
        array_remove(array_agg(distinct nullif(pm.phone31,'') ||pm.phone32 ||  pm.phone33),null) as phone3,
        array_remove(array_agg(distinct nullif(lower(pm.email),'') ),null)    as pm_email,
        array_remove(array_agg(distinct nullif(lower(case when rpm.email ilike '%ggvcp%' then null else rpm.email end ),'') ),null)   as rp_email,
        array_remove(array_agg(distinct nullif(rpm.phone11,'') ||rpm.phone12 ||  rpm.phone13),null) as rp_phone,
        array_remove(array_agg(distinct nullif(rpm.fax1,'') ||rpm.fax2 ||  rpm.fax3),null) as fax
	FROM ips.patient_master pm
	    left join ips.responsible_party_master rpm on pm.account_id=rpm.srno
	where account_id is not null
	    group by pm.account_id
	)
			 
select 
    account_id,
    array_cat(array_cat(array_cat(phone1,phone2),phone3),rp_phone) as phone_numbers,
    array_cat(pm_email,rp_email) as emails,
    fax
from contacts