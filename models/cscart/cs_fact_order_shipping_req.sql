{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_ord_id on {{this.schema}}.{{this.name}} (order_id)")]
  })
}}
select  distinct on (h.order_id)
		h.order_id,
		p.shipping_requirement,
		case 
			when  p.shipping_requirement ilike '%Overnight%'
			then 1 
			else 2 
		end rank
from analytics_cscart.cs_fact_order_header h
join analytics_cscart.cs_fact_order_lines l on h.order_id=l.order_id
join analytics_cscart.cs_dim_product p on l.product_id=p.product_id

order by 
	h.order_id,
	rank asc
