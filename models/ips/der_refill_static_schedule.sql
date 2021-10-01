select 
    p.rxno,
    p.patient_id,
    p.doctor_id,
    p.drug_id,
    'scheduled' as scenario,
    p.schedule_type,
    generate_series(+1,  rs.remaining_refills-1) as fill_number,
    generate_series((p.start_date+p.days_supply::integer), 
         p.start_date + ((rs.remaining_refills)*p.days_supply::integer)::integer
        ,(p.days_supply||' day')::interval) as refill_date,
    p.qty,
    rs.prescription_avg_price as amount,
    rs.start_date

FROM    {{ ref('fact_prescription') }} p
join {{ ref('calc_refill_status') }} rs on p.rxno=rs.rxno
where   p.is_first_fill and
p.transaction_type ='Prescription'
and p.days_supply !=0
and rs.remaining_refills!=0