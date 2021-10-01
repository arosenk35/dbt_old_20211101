{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_rxno on {{this.schema}}.{{this.name}} (rxno)"),
      after_commit("create index  index_{{this.name}}_on_scenario on {{this.schema}}.{{this.name}} (scenario,rxno)"),
      after_commit("create index  index_{{this.name}}_on_firstfill on {{this.schema}}.{{this.name}} (is_first_fill)")
      ]
  })
  }}
  select  rxno,
        patient_id,
        doctor_id,
        drug_id,
        scenario, 
        fill_number,
        refill_date,
        qty,
        amount,
        rxno::text||fill_number as refill_id,
        schedule_type,
        false::boolean as is_first_fill,
        case 
            when   refill_date is null then '0'
            when   refill_date-start_date <=interval '15 days' then '15'
            when   refill_date-start_date <=interval '30 days' then '30'
            when   refill_date-start_date <=interval '60 days' then '60'
            when   refill_date-start_date <=interval '90 days' then '90'
            when   refill_date-start_date <=interval '120 days' then '120'
            when   refill_date-start_date <=interval '150 days' then '150'
            when   refill_date-start_date <=interval '180 days' then '180'
            when   refill_date-start_date <=interval '210 days' then '210'
            when   refill_date-start_date <=interval '240 days' then '240'
            when   refill_date-start_date <=interval '270 days' then '270'
            when   refill_date-start_date <=interval '300 days' then '300'
            when   refill_date-start_date <=interval '330 days' then '330'
            when   refill_date-start_date <=interval '360 days' then '360'
            else '360+'
        end as days_since_first_fill_tier
    from {{ ref('der_refill_static_schedule') }}
union all
    select   
        p.rxno,
        p.patient_id,
        p.doctor_id,
        p.drug_id,
        p.scenario, 
        p.fill_number,
        p.dispense_date as refill_date,
        p.qty,
        p.amount,
        p.refill_id,
        p.schedule_type,
        p.is_first_fill,
        p.days_since_first_fill_tier
    FROM {{ ref('fact_prescription') }} p
    where  transaction_type ='Prescription'