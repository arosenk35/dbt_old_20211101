
{{
  config({
    "materialized": "table",
    "post-hook": [ after_commit("create index  index_{{this.name}}_on_rxno on {{this.schema}}.{{this.name}} (rxno)")
      ]
  })
  }}
  select distinct on (rx_id)
     rx_id as rxno,
     drug as shipping_method,
	 (drug ilike '%pri%over%') as priority
from ips.prescription p
where p.rx_id  like 'otc%'
    and p.office_id = 2
    and p.active='Y'
order by 
    rx_id,
    created_date desc