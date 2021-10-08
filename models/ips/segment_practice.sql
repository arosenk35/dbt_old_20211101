{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (practice_id)")]
  })
  }}

select 
        b.practice_id,
        count (distinct b.doctor_id)  as nbr_vets,
        count (distinct b.patient_id) as nbr_patients,
        count (distinct b.practice_id)   as nbr_practices,
        count (distinct b.drug_id)    as nbr_drugs,
        count (distinct b.account_id) as nbr_owners,
        count (distinct b.refill_id)  as ltv_refills_count,
        count (distinct b.rxno)       as ltv_pescriptions_count,
        count (distinct b.rxno) FILTER (WHERE s.opportunity_ranking >10 ) as open_opportunities_count,
        sum (amount)            FILTER (WHERE s.opportunity_ranking >10 ) as open_opportunities_amount,
        count (distinct b.rxno) FILTER (WHERE s.refill_status ='Open' )   as open_prescriptions_count,
        sum (amount)            FILTER (WHERE s.refill_status ='Open' )   as open_prescriptions_amount,
        min(b.start_date)     as min_start_date,
        min(b.dispense_date)  as min_dispense_date,
        max(b.dispense_date)  as max_dispense_date
FROM {{ ref('fact_prescription') }} b
join {{ ref('der_refill_status') }} s on s.rxno=b.rxno
group by b.practice_id