{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_rxno on {{this.schema}}.{{this.name}} (rxno)")
      ]
  })
  }}
  with refills as
    (select 
	    fill_number,
	    rx_expire_date,
        patient_id,
        doctor_id,
        rxno,
        drug_id,
        no_of_refill,
        case 
            when fill_number=0 then  start_date 
            else dispense_date 
        end as status_date,
        days_supply,
        is_first_fill,
        is_last_fill,
        start_date,
        amount

    FROM {{ ref('fact_prescription') }}
    where  transaction_type ='Prescription'
union all
    (select 
	 distinct on (p.rxno)
	    p.fill_number,
	    p.rx_expire_date,
        p.patient_id,
        p.doctor_id,
        p.rxno,
        p.drug_id,
        p.no_of_refill,
        p.rx_expire_date::date as status_date,
        p.days_supply,
        p.is_first_fill,
        p.last_fill,
        p.start_date,
        p.amount

    FROM {{ ref('fact_prescription') }} p
    join {{ ref('calc_refill_status') }} rs on p.rxno=rs.rxno
    where p.transaction_type ='Prescription'
    and rs.refill_status='Expired'
	 order by rxno,fill_number desc)
    )
	
    select 
        distinct on (rxno,status_date)
		fill_number,
		rx_expire_date,
        patient_id,
        doctor_id,
        rxno,
        drug_id,
        no_of_refill,
        status_date,
        days_supply,
        is_first_fill,
        is_last_fill,
        start_date,
        amount
    from refills
    order by rxno,status_date asc