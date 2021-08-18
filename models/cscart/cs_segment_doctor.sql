{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_p_id on {{this.schema}}.{{this.name}} (doctor_id)")]
  })
}}
SELECT  (coalesce(nullif(cs.vet_data__id,''),'U'||cs.order_id) )            as doctor_id,
		min(TIMESTAMP 'epoch' + timestamp::numeric * INTERVAL '1 second')   as first_order_date,
		max(TIMESTAMP 'epoch' + timestamp::numeric * INTERVAL '1 second')   as last_order_date,
        count(distinct order_id)                                            as nbr_orders

FROM cscart.orders cs
group by (coalesce(nullif(cs.vet_data__id,''),'U'||cs.order_id) ) 