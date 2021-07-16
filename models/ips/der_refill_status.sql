{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_rxno on {{this.schema}}.{{this.name}} (rxno,refill_status)"),
      after_commit("create index  index_{{this.name}}_on_startdate on {{this.schema}}.{{this.name}} (start_date)")
      ]
  })
  }}
  select  
        rxno
        patient_id,
        doctor_id,
        drug_id,
        rxno,
        no_of_refill,
        refill_status,
        rx_expire_date,
        days_supply,
        next_refill_date,
        nfo_status,
        schedule_type,
        start_date,
        previous_rxno,
        previous_rx_date,
        next_rxno,
        next_rx_date,
        nbr_renewals,
        prescription_renewal,
        opportunity_stage,
        --case 
        --    when opportunity_ranking = 0 then 0
        --    when api_category = 'Chronic' then opportunity_ranking+5000
        --    when api_category = 'Accute' then opportunity_ranking+4000
        --    else  opportunity_ranking
        --end 
        opportunity_ranking,
        renewal_status,
        prescription_price,
        prescription_avg_price,
        actual_no_of_refill,
        over_prescribed,
        dispensed_refill_amount,
        dispensed_first_fill_amount,
        dispensed_total_amount,
        dispensed_refill_qty,
        dispensed_first_fill_qty,
        dispensed_total_qty,
        dispensed_nbr_refills,
        prescribed_refill_scheduled_amount,
        prescribed_refill_amount,
        prescribed_total_fill_amount,
        prescribed_refill_scheduled_qty,
        prescribed_refill_qty,
        prescribed_total_fill_qty,
        master_rxno,
        origin,
        auto_fill
        
from {{ ref('calc_refill_status') }}