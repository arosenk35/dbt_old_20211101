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
    strength_value
FROM ips.drug_master 
    where nullif(btrim(drug),'') is not null
)
,
cscart as ( 
select
    nullif(regexp_replace(lower(cs.product),' |otic|%|s$|oral|\(.*\)$','','g'),'') as key,
    cs.*
from cscart.products cs
    where nullif(btrim(product),'') is not null
)

select distinct on(cs.product_id)
    cs.key,
    cs.product,
    cs.product_id,
    cs.price,
    TIMESTAMP 'epoch' + timestamp::numeric * INTERVAL '1 second' as created_date,
    ips.drug_id,
    ips.drug_form,
    ips.strength,
    ips.strength_value,
    ips.drug
from cscart cs
left join  ips on ips.key=cs.key
order by cs.product_id,
timestamp desc
