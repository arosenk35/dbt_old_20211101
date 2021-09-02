{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_prod_key on {{this.schema}}.{{this.name}} (key)")]
  })
}}



select distinct on(cs.product_id)
    cs.key,
    cs.product ,
    cs.product_id ,
    cs.price,
	cs.product_code,
    cs.status as status,
    status_name,
    cs.updated_date,
	cs.created_date,
	(ips.drug_id is not null) as ips_found,
	(coalesce(cs.product_code,'cs')=coalesce(ips.quick_code,'ips')) as cs_ips_codes_match,
    cs.image_path,
    cold_shipping,
	ips.quick_code as ips_quick_code,
    ips.drug_id as ips_drug_id,
    ips.drug_form as ips_drug_form,
    ips.strength as ips_strength,
    ips.strength_value as ips_strength_value,
    ips.master_drug as ips_drug,
	ips.active,
	case 
	when ips.active='N' then 99
	when ips.drug_key1=cs.key then 1
	when ips.drug_key2=cs.key then 2
	when ips.drug_key3=cs.key then 3
	else 88
	end as rank
	
from {{ ref('cs_der_product') }} cs
left join {{ ref('dim_drug') }} ips  on (
	ips.drug_key1=cs.key or
	ips.drug_key2=cs.key or
	ips.drug_key3=cs.key )
order by cs.product_id,
   rank desc