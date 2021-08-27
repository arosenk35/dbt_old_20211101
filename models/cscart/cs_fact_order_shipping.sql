{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_ord_id on {{this.schema}}.{{this.name}} (order_id)")]
  })
}}

SELECT  _sdc_source_key_order_id as order_id,
        shipping as shipping_method,
        rate::numeric,
        case 
            when shipping ilike '%pickup%' 
                  then 'Standard' 
            when shipping ilike '%expedited%' 
                then 'Overnight'
            when shipping ilike '%overnight%' 
                then 'Overnight'
            else 'Standard'
        end as method

FROM cscart.orders__shipping