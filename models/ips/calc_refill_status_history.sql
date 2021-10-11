  {{
  config({
    "materialized": "table",
    "post-hook": [
      after_commit("create index  index_{{this.name}}_on_rxno on {{this.schema}}.{{this.name}} (rxno,status_date)"),
      after_commit("create index  index_{{this.name}}_on_dates on {{this.schema}}.{{this.name}} (start_date,end_date)")
      ]
  })
  }}

  
select 
distinct on (rxno,status_date)
    s.fill_number,
    s.status_date,
    s.patient_id,
    s.doctor_id,
    s.rxno,
    s.drug_id,
    s.no_of_refill,
    s.days_supply,
    s.is_first_fill,
    s.rx_expire_date,
    s.is_last_fill,
    s.amount,
    s.start_date,
    case  
        when s.no_of_refill<=s.fill_number  then s.status_date
        when (s.no_of_refill>s.fill_number and status_date::date>=s.rx_expire_date::date) 
        then s.status_date
        when round(date_part('day',status_date::timestamp-rs.first_fill_date)/nullif(s.days_supply,0))-s.fill_number >3
        and  rs.last_refill_date < now()-interval '2 month'
        then s.status_date
    end as end_date, 
    case  
        when s.no_of_refill<=s.fill_number  then 'Complete'
        when (s.no_of_refill>s.fill_number and status_date::date>=s.rx_expire_date::date) 
        then 'Expired'
        when round(date_part('day',status_date::timestamp-rs.first_fill_date)/nullif(s.days_supply,0))-s.fill_number >3
        and  rs.last_refill_date < now()-interval '2 month'
        then 'Lost'
        else 'Open'
    end as refill_status,
    case  
        when s.no_of_refill<=s.fill_number  then 1
        when (s.no_of_refill>s.fill_number and s.status_date::date>=s.rx_expire_date::date) 
        then 3
        when round(date_part('day',status_date::timestamp-rs.first_fill_date)/nullif(s.days_supply,0))-s.fill_number >3
        and  rs.last_refill_date < now()-interval '2 month'
        then 4
        else 2
    end as priority
    from {{ ref('calc_refill_status_sequence') }} s
    join {{ ref('calc_refill_status') }} rs on s.rxno=rs.rxno
    order by rxno,status_date asc, priority asc