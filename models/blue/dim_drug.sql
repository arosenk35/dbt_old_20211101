{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (drug_id)"),
      after_commit("create index  index_{{this.name}}_on_drug on {{this.schema}}.{{this.name}} (master_drug)")]
  })
  }}
    SELECT distinct on (drug_id)

    dm.drug_id,
    dm.drug||' '||dm.strength_value||' '||dm.strength as drug_name, 
    dm.drug as master_drug, 
    dm.ndc, 
    btrim(dm.drug_form) as drug_form, 
    btrim(dm.strength) as strength, 
    dm.strength_value, 
    dm.generic, 
    dm.manufacturer, 
    dm.quick_code, 
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
    u.upc_code, 
    dm.awp,
    coalesce(da.api_category,'Unclassified') as api_category,
    da.controlled,
    da.common,
    dm.active,
    CASE when dm.drug ilike '%fedex%' then 'shipping'
          when dm.drug ilike '%usps%' then 'shipping'
          else 'drug'
    END as item_type,
    p.created_date as last_used_date
FROM ips.drug_master dm
join ips.prescription p on dm.drug_id=p.drug_id
left join ips.drug_master_upc u on dm.ndc=u.ndc
left join {{ ref('dim_api_category') }} da on dm.drug  ilike '%'||da.master_drug||'%'
order by drug_id,p.created_date desc