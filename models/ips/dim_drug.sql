{{
  config({
    "materialized": "table",
    "post-hook": [
        after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (drug_id)"),
        after_commit("create index  index_{{this.name}}_on_drug on {{this.schema}}.{{this.name}} (master_drug)"),
        after_commit("create index  index_{{this.name}}_on_drug_k1 on {{this.schema}}.{{this.name}} (drug_key1)"),
        after_commit("create index  index_{{this.name}}_on_drug_k2 on {{this.schema}}.{{this.name}} (drug_key2)"),
        after_commit("create index  index_{{this.name}}_on_drug_k3 on {{this.schema}}.{{this.name}} (drug_key3)"),
        after_commit("create index  index_{{this.name}}_on_drug_k4 on {{this.schema}}.{{this.name}} (drug_key4)"),
        after_commit("create index  index_{{this.name}}_on_quickcode on {{this.schema}}.{{this.name}} (quick_code)")
      ]
  })
  }}
    SELECT distinct on (drug_id)

    lower(regexp_replace(coalesce(drug,'')||coalesce(strength_value,'')||coalesce(strength,'')||coalesce(drug_form,''),'otic|\,|\-|\(|\)|\/| |\%|s$|oral|','','g'))
            as drug_key1,
    lower(regexp_replace(coalesce(drug,'')||coalesce(strength_value,'')||coalesce(strength,'')||coalesce(drug_form,''),'|\(.*\)$|otic||\,|\-|\(|\)|\/| |\%|s$|oral|','','g'))
            as drug_key2,
    lower(regexp_replace(coalesce(drug,'')||coalesce(strength_value,'')||coalesce(strength,'')||coalesce(drug_form,''),'SULFATE|HCL|\(.*\)$|otic|\,|\-|\(|\)|\/| |\%|s$|oral|','','g'))
            as drug_key3,
    replace(lower(regexp_replace(coalesce(drug,'')||coalesce(strength_value,'')||coalesce(strength,'')||coalesce(drug_form,''),'SULFATE|HCL|\(.*\)$|otic|\,|\-|\(|\)|\/| |\%|s$|oral|','','g'))
        ,'solution','suspension')
        as drug_key4,
   case when drug ilike '%topi%click%'
        then
        lower(regexp_replace(coalesce(drug,''),'|\,|\-|\(|\)|\/| |\%|s$|oral|','','g'))
    end as drug_key_topi,

    dm.drug_id,
    btrim(coalesce(dm.drug,'')||' '||coalesce(dm.strength_value,'')||' '||coalesce(dm.strength,'')) as drug_name, 
    dm.drug                   as master_drug, 
    dm.ndc, 
    btrim(dm.drug_form)       as drug_form, 
    btrim(dm.strength)        as strength, 
    dm.strength_value, 
    dm.generic, 
    dm.manufacturer, 
    lower(dm.quick_code) as quick_code, 
    dm.qty, 
    dm.qty_pack, 
    dm.color, 
    dm.shape, 
    dm.flavor, 
    dm.drug_class_group,  
    dm.created_date,  
    dm.changed_date,  
    dm.med_type, 
    dm.acquisition_cost, 
    dm.primary_supplier, 
    dm.secondary_supplier, 
    dm.price_template_id,  
    dm.special_type, 
    dm.drug_subtype,
    dm.awp,
    coalesce(da.api_category,'Unclassified') as api_category,
    da.controlled,
    da.common,
    dm.active,
    CASE when dm.drug ilike '%fedex%' 
          then 'shipping'
          when dm.drug ilike '%usps%' 
          then 'shipping'
          else 'drug'
    END as item_type
FROM ips.drug_master dm
left join {{ ref('dim_api_category') }} da on dm.drug  ilike '%'||da.master_drug||'%'
order by drug_id