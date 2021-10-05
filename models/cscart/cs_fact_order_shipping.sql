{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_ord_id on {{this.schema}}.{{this.name}} (order_id)")]
  })
}}

SELECT  _sdc_source_key_order_id as order_id,
        shipping as shipping_method_orig,
        btrim(nullif(regexp_replace((shipping),'Discount|\(.*\)$','','g'),'') ) as shipping_method,
        rate::numeric,
        case 
            when shipping ilike '%pickup%' 
                  then 'Pickup' 
            when shipping ilike '%free%' 
                  then 'Free' 
            when shipping ilike '%expedited%' 
                then 'Overnight'
            when shipping ilike '%overnight%' 
                then 'Overnight'
            else 'Standard'
        end as shipping_class,
        case 
            when shipping ilike '%fedex%'
            then 'FEDEX'
            when shipping ilike '%ups%'
            then 'UPS'
            when shipping ilike '%usps%'
            then 'USPS'
            when shipping ilike '%dhl%'
            then 'DHL'
        end sipping_carrier,
        nullif(delivery_time,'')  as delivery_time


FROM cscart.orders__shipping