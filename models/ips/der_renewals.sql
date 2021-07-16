{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_rxno on {{this.schema}}.{{this.name}} (rxno)")
      ]
  })
  }}
with refill as (
  SELECT  rx_id as rxno,
          row_number()              OVER (PARTITION BY patient_id, drug_id order by created_date) as rx_renewal_sequence,
          first_value(rx_id)        OVER (PARTITION BY patient_id, drug_id order by created_date) as master_rxno,	
          first_value(created_date) OVER (PARTITION BY patient_id, drug_id order by created_date) as master_rx_start_date,
          last_value(created_date)  OVER (PARTITION BY patient_id, drug_id order by created_date) as last_rx_date,
          lag(rx_id)                OVER (PARTITION BY patient_id, drug_id order by created_date) as previous_rxno,
          lag(start_date)           OVER (PARTITION BY patient_id, drug_id order by created_date) as previous_rx_date,
          lead(rx_id)               OVER (PARTITION BY patient_id, drug_id order by created_date) as next_rxno,
          lead(start_date)          OVER (PARTITION BY patient_id, drug_id order by created_date) as next_rx_date,
          count(*)                  OVER (PARTITION BY patient_id, drug_id) as nbr_renewals
  FROM ips.prescription 
        where 
            rx_id not like 'otc%' and
            office_id = 2 and
            active='Y'
)
select *,
case when nbr_renewals=1 then false else true end as prescription_renewal
from refill 
where nbr_renewals>1