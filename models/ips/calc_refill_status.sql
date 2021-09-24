{{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_rxno on {{this.schema}}.{{this.name}} (rxno,refill_status)"),
      after_commit("create index  index_{{this.name}}_on_startdate on {{this.schema}}.{{this.name}} (start_date)")
      ]
  })
  }}
  
  with last_refill as
    (select 
        distinct on (p.rxno)
        p.patient_id,
        p.doctor_id,
        p.account_id,
        p.nfo_status,
        p.rxno,
        p.fill_number,
        p.tran_id as last_tran_id,
        p.dispense_date as last_dispense_date,
        p.drug_id,
        p.no_of_refill,
        p.hold_date,
        p.hold_note,
        p.sig,
        p.flavor,
        case 
            when no_of_refill-fill_number <0 then 0
            else no_of_refill-fill_number
        end as remaining_refills,
        case 
            when no_of_refill-fill_number <0 then 0
            else fill_number+1
        end as next_fill_number,
        rx_expire_date,
        (now()>=p.rx_expire_date and p.no_of_refill!=p.fill_number) as expired,
        p.dispense_date as last_refill_date,
        p.days_supply,
        case 
            when p.no_of_refill=fill_number 
            then null
            when now()>=p.rx_expire_date 
            then null
            when (p.dispense_date::date + days_supply) >= p.rx_expire_date 
            then rx_expire_date
            else p.dispense_date::date + p.days_supply 
        end as next_refill_date,
        p.first_fill,
        p.schedule_type,
        p.start_date,
        p.qty ,       
        p.prescription_price,
        dmd.api_category,
        auto_fill,
        prescription_type,
        p.origin


FROM {{ ref('fact_prescription') }} p
join{{ ref('dim_drug') }}  dmd on dmd.drug_id=p.drug_id
where  p.transaction_type ='Prescription'
order by p.rxno, p.fill_number desc
),

first_fill as (
    select rxno,
    min(dispense_date) as first_fill_date,
    sum(amount) as dispensed_total_amount,
    sum(amount) filter (where fill_number>0)        as dispensed_refill_amount,
    sum(amount) filter (where fill_number=0)        as dispensed_first_fill_amount,
    sum(qty)                                        as dispensed_total_qty,
    count(refill_id) filter (where fill_number>0)   as dispensed_nbr_refills,
    sum(qty) filter (where fill_number>0)           as dispensed_refill_qty,
    sum(qty) filter (where fill_number=0)           as dispensed_first_fill_qty,
    sum(amount)/count(*) as prescription_avg_price,
    max(fill_number) as last_fill_number,
    case 
        when max(fill_number) >avg(no_of_refill) 
        then max(fill_number) 
        else avg(no_of_refill)
    end as actual_no_of_refill
    FROM {{ ref('fact_prescription') }}
    where  transaction_type ='Prescription'
    group by rxno
)

select  distinct on (rxno)
        l.*,
        r.nbr_renewals,
        r.previous_rxno,
        r.previous_rx_date,
        r.last_rx_date,
        r.next_rxno,
        r.next_rx_date,
        r.master_rxno,
        r.master_rx_start_date,
        coalesce(r.prescription_renewal,false) as prescription_renewal,
        f.first_fill_date ,
		case 
            when pat.dod is not null then 'Deceased'
            when fill_number=-1 and hold_date is not null then 'OnHold' 
            when fill_number=-1 then 'InProgress'
            when no_of_refill<=fill_number then 'Complete'
            when (no_of_refill>fill_number and now()>=rx_expire_date) then 'Expired'
            when round(date_part('day',now()-f.first_fill_date)/nullif(days_supply,0))-fill_number >3
            and  last_refill_date < now()-interval '2 month'
            then 'Lost'
            else 'Open'
        end as refill_status,
        case 
            when pat.active = false then 'Lost'
            when pat.dod is not null then 'Lost'
            when fill_number=-1 and hold_date is not null then 'OnHold' 
            when fill_number=-1 then 'InProgress'
            when no_of_refill<=fill_number then 'Complete'
            when (no_of_refill>fill_number and now()>=rx_expire_date) then 'Expired'
            when round(date_part('day',now()-f.first_fill_date)/nullif(days_supply,0))-fill_number >3
            and  last_refill_date < now()-interval '2 month'
		then 'Lost'
        else 'Open'
        end as opportunity_stage,
		case 
            when pat.dod is not null then 0
            when fill_number=-1 and hold_date is not null then 1
            when fill_number=-1 then 1
            when (no_of_refill>fill_number and now()>=rx_expire_date and r.prescription_renewal) then 29
            when (no_of_refill>fill_number and now()>=rx_expire_date) then 20
            when no_of_refill<=fill_number and r.next_rxno is not null then 1
            when no_of_refill<=fill_number and r.prescription_renewal then 99
            when no_of_refill<=fill_number and r.prescription_renewal is null then 90
            when (no_of_refill>fill_number and (rx_expire_date between now() and now()+ interval '2 months') and r.prescription_renewal) then 69
            when (no_of_refill>fill_number and (rx_expire_date between now() and now()+ interval '2 months')) then 60
            when round(date_part('day',now()-f.first_fill_date)/nullif(days_supply,0))-fill_number >3
            and  last_refill_date < now()-interval '2 months' and r.prescription_renewal
            then 59
            when round(date_part('day',now()-f.first_fill_date)/nullif(days_supply,0))-fill_number >3
            and  last_refill_date < now()-interval '2 month' 
            then 50
            else 0
        end as opportunity_ranking,

        case 
            when r.next_rxno is null then 'Unrenewed'
            else 'Renewed'
        end as renewal_status,
        f.dispensed_refill_amount,
        f.dispensed_first_fill_amount,
        f.dispensed_total_amount,
        f.dispensed_refill_qty,
        f.dispensed_first_fill_qty,
        f.dispensed_total_qty,
        f.dispensed_nbr_refills,
        f.prescription_avg_price,
        f.actual_no_of_refill,
        f.last_fill_number,
        prescription_avg_price*(actual_no_of_refill-fill_number) AS prescribed_refill_scheduled_amount,
        actual_no_of_refill*prescription_avg_price as prescribed_refill_amount,
        (actual_no_of_refill+1)*prescription_avg_price as prescribed_total_fill_amount,
        qty*(actual_no_of_refill-fill_number) AS prescribed_refill_scheduled_qty,
        actual_no_of_refill*qty as prescribed_refill_qty,
        (actual_no_of_refill+1)*qty as prescribed_total_fill_qty,
        (actual_no_of_refill>no_of_refill) as over_prescribed,
        product_renewal
from last_refill l
join first_fill f on l.rxno=f.rxno
left join  {{ ref('dim_patient') }} pat on l.patient_id=pat.patient_id
left join {{ ref('der_renewals') }} r on r.rxno=l.rxno