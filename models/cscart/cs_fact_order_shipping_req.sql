{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_ord_id on {{this.schema}}.{{this.name}} (order_id)")]
  })
}}
select  distinct on (l.order_id)
		l.order_id,
		p.cold_shipping,
		case 
			when  p.cold_shipping
			then 1 
			else 2 
		end rank
from {{ ref('cs_fact_order_lines') }} l 
join {{ ref('cs_dim_product') }} p on l.product_id=p.product_id

order by 
	l.order_id,
	rank asc