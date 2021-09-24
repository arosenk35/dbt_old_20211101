{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_ord_id on {{this.schema}}.{{this.name}} (order_id)")]
  })
}}
select
        l.order_id,
        l.item_id           as line_id,
        l.product,
        l.product_id,
        l.tax_value         as tax_amount,
        l.discount,
        l.subtotal          as gross_amount,
        l.price             as price,
        l.amount            as quantity_ordered,
        (select 
              opt.variant_name 
          from cscart.orders__lines__extra__product_options_value opt
              where opt._sdc_source_key_order_id=l._sdc_source_key_order_id 
              and option_name ilike '%strength%'
              and l._sdc_level_0_id =opt._sdc_level_0_id
              and opt.status='A' and nullif(variant_name,'') is not null
              limit 1) 
        as strength,
        (select 
              initcap(opt.variant_name)
          from cscart.orders__lines__extra__product_options_value opt
              where opt._sdc_source_key_order_id=l._sdc_source_key_order_id 
              and option_name ilike '%flavor%'
              and l._sdc_level_0_id =opt._sdc_level_0_id
              and opt.status='A' and nullif(variant_name,'') is not null
              limit 1) 
        as flavor,
        (select 
              lower(nullif(opt.variant_name,'-'))
          from cscart.orders__lines__extra__product_options_value opt
              where opt._sdc_source_key_order_id=l._sdc_source_key_order_id 
              and option_name ilike '%instruct%'
              and l._sdc_level_0_id =opt._sdc_level_0_id
              and opt.status='A' and nullif(variant_name,'') is not null
              limit 1) 
        as instruction,
        (select 
              opt.variant_name 
          from cscart.orders__lines__extra__product_options_value opt
        where opt._sdc_source_key_order_id=l._sdc_source_key_order_id 
            and opt.option_name ilike '%quantity%' 
            and l._sdc_level_0_id =opt._sdc_level_0_id
            and opt.status='A' and nullif(variant_name,'') is not null
            limit 1) 
        as quantity,
        (select 
            regexp_replace(substring(opt.option_name from '(\(.*\))'),'\(|\)','','g') as price_plan
          from cscart.orders__lines__extra__product_options_value opt
        where opt._sdc_source_key_order_id=l._sdc_source_key_order_id 
            and l._sdc_level_0_id =opt._sdc_level_0_id
            and opt.status='A' 
            and option_type='S'
            and opt.modifier_type='A' 
            and option_name like '(%)%' 
            and modifier::numeric !=0
            limit 1) 
        as price_plan_code,
        p.drug_form

from cscart.orders__lines l 
left join {{ ref('cs_der_product') }} p on l.product_id=p.product_id