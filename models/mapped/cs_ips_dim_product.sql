{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_product_id on {{this.schema}}.{{this.name}} (product_id)")]
  })
}}



select distinct on(cs.product_id)
    cs.*,
	ips.quick_code as ips_quick_code,
    ips.drug_id as ips_drug_id,
    ips.drug_form as ips_drug_form,
    ips.strength as ips_strength,
    ips.strength_value as ips_strength_value,
    ips.master_drug as ips_drug,
	ips.active,
    case 
        when ips.price_plan_id is null 
        then 9
        else 0
    end price_rank,
    case 
        when is_compound 
        then 0 
        else 9
    end item_type_rank,
	case 
        when ips.active='N' then 99
        when ips.drug_key1=cs.key1 then 1
        when ips.drug_key2=cs.key1 then 2
        when ips.drug_key3=cs.key1 then 3
        when ips.drug_key1=cs.key2 then 4
        when ips.drug_key2=cs.key2 then 5
        when ips.drug_key3=cs.key2 then 6
        when ips.drug_key4=cs.key1 then 7
        when ips.drug_key4=cs.key2 then 8
        when ips.drug_key_topi=cs.key_topi then 9  
        when ips.quick_code=cs.product_code then 77
        else 88
	end as rank
	
from {{ ref('cs_der_product') }} cs
left join {{ ref('dim_drug') }} ips  on (
	ips.drug_key1=cs.key1 or
	ips.drug_key2=cs.key1 or
	ips.drug_key3=cs.key1 or
    ips.drug_key1=cs.key2 or
	ips.drug_key2=cs.key2 or
	ips.drug_key3=cs.key2 or
    ips.drug_key4=cs.key1 or
    ips.drug_key4=cs.key2 or 
    ips.drug_key_topi=cs.key_topi or
    ips.quick_code=cs.product_code  )
order by cs.product_id,
   price_rank desc,item_type_rank,rank desc