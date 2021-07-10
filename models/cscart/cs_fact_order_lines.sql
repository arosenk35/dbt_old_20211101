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
        l.shipped_amount    as units_shipped,
        l.amount            as units_ordered,
        l.discount,
        l.subtotal          as gross_amount,
        l.price             as unit_price

from cscart.orders__lines l 