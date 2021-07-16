{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_rxno on {{this.schema}}.{{this.name}} (rxno)")
      ]
  })
  }}


with refill as (
  SELECT  	
        p.rx_id as rxno,
		    t.rx_id as last_tran_rxno,
			  case when coalesce(p.last_tran_id,0) = 0  
				  then false 
				  else true
			  end nfo_status,
        p.last_tran_id,
        row_number()                OVER (PARTITION BY p.patient_id, p.drug_id order by p.created_date) as rx_renewal_sequence,
        first_value(p.rx_id)        OVER (PARTITION BY p.patient_id, p.drug_id order by p.created_date) as master_rxno,	
        first_value(p.created_date) OVER (PARTITION BY p.patient_id, p.drug_id order by p.created_date) as master_rx_start_date,
        last_value(p.created_date)  OVER (PARTITION BY p.patient_id, p.drug_id order by p.created_date) as last_rx_date,
        lag(p.rx_id)                OVER (PARTITION BY p.patient_id, p.drug_id order by p.created_date) as previous_rxno,
        lag(p.start_date)           OVER (PARTITION BY p.patient_id, p.drug_id order by p.created_date) as previous_rx_date,
        lead(p.rx_id)               OVER (PARTITION BY p.patient_id, p.drug_id order by p.created_date) as next_rxno,
        lead(p.start_date)          OVER (PARTITION BY p.patient_id, p.drug_id order by p.created_date) as next_rx_date,
        count(*)                    OVER (PARTITION BY p.patient_id, p.drug_id) as nbr_renewals
  FROM ips.prescription p
	left join  ips.prescription t on t.tran_id=	p.last_tran_id
        where 
            p.rx_id not like 'otc%' and
            p.office_id = 2 
)
select 
      rxno,
      nfo_status,
      last_tran_id,
      rx_renewal_sequence,
      case 
        when nbr_renewals=1 
        then last_tran_rxno
        else master_rxno 
      end as master_rxno,	
      master_rx_start_date,
      last_rx_date,
      previous_rxno,
      previous_rx_date,
      next_rxno,
      next_rx_date,
      nbr_renewals,
      case 
        when nbr_renewals=1 
        then false 
        else true 
      end as product_renewal,
      case 
        when nbr_renewals=1 
        then false 
        else true 
      end as prescription_renewal
from refill 
where 
  (nbr_renewals>1 or 
  nullif(last_tran_id,0) is not null)
