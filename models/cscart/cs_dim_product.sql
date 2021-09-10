{{
  config({
    "materialized": "view"
  })
}}

select 
        cs.*,
        md.ips_drug,
        md.ips_drug_form,
        md.ips_strength,
        md.ips_strength_value

from {{ ref('cs_der_product') }} cs
left join {{ ref('cs_ips_dim_product') }} md on cs.product_id=md.product_id