{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_acct_id on {{this.schema}}.{{this.name}} (option_id)")]
  })
}}

SELECT distinct on (option_id)
option_id,
option_name	,
case 
    when  option_type='S' and modifier::numeric !=0 and   option_name like '(%)%'
    then  regexp_replace(substring(option_name from '(\(.*\))'),'\(|\)','','g') 
end as price_plan
FROM cscart.orders__lines__extra__product_options_value
order by option_id, _sdc_received_at desc