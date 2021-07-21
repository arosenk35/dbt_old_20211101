{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_prod_id on {{this.schema}}.{{this.name}} (product_id)")]
  })
}}

select 
    l.product_id ,
    max(TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second') as last_order_date,
    sum(l.amount) as ltv_qty,
    sum(l.subtotal) as ltv_amount,
    count(distinct o.order_id) as nbr_orders,
    count(distinct o.user_id) as nbr_customers

from cscart.orders o
join cscart.orders__lines l on o.order_id=l._sdc_source_key_order_id
group by l.product_id