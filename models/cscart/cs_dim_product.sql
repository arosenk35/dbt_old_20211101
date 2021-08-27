{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_prod_id on {{this.schema}}.{{this.name}} (product_id)")]
  })
}}

with ips as (
select 
case 
	when lower(regexp_replace(drug,' |%|','','g')) ilike '%'||lower(regexp_replace(coalesce(strength_value,''),' |%|','','g'))||replace(coalesce(strength,''),' ','')||regexp_replace(coalesce(drug_form,''),' ','','g') 
    then nullif(lower(regexp_replace(drug,' |otic|%|','','g')),'')
    else
         nullif(lower(regexp_replace(drug,' |otic|%|','','g'))||lower(regexp_replace(coalesce(strength_value,''),' |%|','','g'))||lower(regexp_replace(coalesce(strength,''),' |%|','','g'))||regexp_replace(lower(coalesce(drug_form,'')),' |s$','','g'),'') 
end as key,
    drug,
    drug_id,
    drug_form,
    strength,
    strength_value,
	quick_code
FROM ips.drug_master 
    where nullif(btrim(drug),'') is not null
)
,
cscart as ( 
select
    nullif(regexp_replace(lower(cs.product),' |otic|%|s$|oral|\(.*\)$','','g'),'') as key,
    cs.*,
    case 
    when 
        product ilike 'AMLODIPINE%SUSPENSION%' or
        product ilike 'ARANESP%(DARBEPOETIN ALFA)%INJECTION VIAL%' or
        product ilike 'POTASSIUM%SUSPENSION%' or
        product ilike 'OMEPRAZOLE%SUSPENSION%' or
        product ilike 'TERBUTALINE%SUSPENSION%' or
        product ilike 'CHLORAMBUCIL%' or
        product ilike 'SAME/MILK%THISTLE%SUSPENSION%' or
        product ilike 'AZITHROMYCIN%SUSPENSION%' or
        product ilike 'MARBOFLOXACIN%SUSPENSION%'
    then 'Overnight'
    else 'Standard'
    end as shipping_requirement
from cscart.products cs
    where nullif(btrim(product),'') is not null
)

select distinct on(cs.product_id)
    cs.key,
    cs.product ,
    cs.product_id ,
    cs.price,
	cs.product_code,
    cs.status as status,
    case    when cs.status='A' then 'Active'
            when cs.status='D' then 'Disabled'
            when cs.status='H' then 'Hidden'
    end status_name,
    TIMESTAMP 'epoch' + updated_timestamp::numeric * INTERVAL '1 second' as updated_date,
	TIMESTAMP 'epoch' + timestamp::numeric * INTERVAL '1 second' as created_date,
	(ips.drug_id is not null) as ips_found,
	(coalesce(cs.product_code,'cs')=coalesce(ips.quick_code,'ips')) as cs_ips_codes_match,
    main_pair__detailed__http_image_path as image_path,
	ips.quick_code as ips_quick_code,
    ips.drug_id as ips_drug_id,
    ips.drug_form as ips_drug_form,
    ips.strength as ips_strength,
    ips.strength_value as ips_strength_value,
    ips.drug as ips_drug,
    shipping_requirement
	
from cscart cs
left join  ips on ips.key=cs.key
order by cs.product_id,
   updated_timestamp desc