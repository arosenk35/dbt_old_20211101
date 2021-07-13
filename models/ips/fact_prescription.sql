{{
  config({
    "materialized": "table",
    "post-hook": [
        after_commit("create index  index_{{this.name}}_on_dispense_date on {{this.schema}}.{{this.name}} (dispense_date)"),
        after_commit("create index  index_{{this.name}}_on_start_date on {{this.schema}}.{{this.name}} (transaction_type,start_date)"),
        after_commit("create index  index_{{this.name}}_on_rxno on {{this.schema}}.{{this.name}} (rxno)"),
        after_commit("create index  index_{{this.name}}_on_doctor on {{this.schema}}.{{this.name}} (doctor_id)"),
        after_commit("create index  index_{{this.name}}_on_patient on {{this.schema}}.{{this.name}} (patient_id)"),
        after_commit("create index  index_{{this.name}}_on_account on {{this.schema}}.{{this.name}} (account_id)"),
        after_commit("create index  index_{{this.name}}_on_practice on {{this.schema}}.{{this.name}} (practice)")
      ]
  })
  }}

  SELECT 
        prescription.patient_id, 
		prescription.drug_id, 
		prescription.doctor_id, 
		prescription.account_id,
        prescription.note, 
        prescription.rx_id as rxno, 
		prescription.rx_id::text||fill_number::text as refill_id, 
        prescription.med_type, 
        prescription.sig, 
        prescription.hold, 
        prescription.hold_note,
        prescription.hold_date,
        prescription.newfromold_label,
        prescription.start_date, 
        coalesce(prescription.price,(bill_header.patient_pay-bill_header.sales_tax_amount)) as prescription_price, 
        prescription.qty, 
        prescription.drug_expire_date,
        prescription.rx_expire_date,
        prescription.tran_id, 
        bill_header.patient_pay-bill_header.sales_tax_amount as amount, 
        prescription.no_of_refill, 
        bill_detail.fill_status, 
        bill_detail.check_status, 
        bill_header.dispense_flag, 
        bill_header.dispense_date, 
        bill_detail.response_message, 
        bill_detail.created_date,
        bill_detail.changed_date, 
        prescription.office_id, 
        prescription.otc, 
        bill_header.fill_number, 
        prescription.sig_english, 
        prescription.ips_bill, 
        bill_detail.delay_reason_code,
        case when coalesce(prescription.last_tran_id,0) = 0  
        then false 
        else true
        end nfo_status,
		case  
            when prescription.rx_id ilike '%otc%' and charge_to_account ='Y'
            then 'Shipping'
            when prescription.rx_id ilike '%otc%' and charge_to_account ='N'
            then 'Samples'
            else 'Prescription'
        end as transaction_type,
        CASE    when prescription.rx_id ilike '%otc%' and charge_to_account ='Y' 
                then 0
                else bill_header.patient_pay-bill_header.sales_tax_amount
        END as prescription_charge,
        CASE when prescription.rx_id ilike '%otc%' and charge_to_account ='Y' 
                then bill_header.patient_pay-bill_header.sales_tax_amount
        END as shipping_charge,
        'actuals' as scenario, 
        coalesce(ips.prescription.days_supply,1)::integer as days_supply,
        bill_header.fill_number=0 as first_fill,
        bill_header.fill_number=1 as fill_schedule_started,
        no_of_refill=fill_number as last_fill,
        case 
            when  prescription.days_supply <=15 then '15'
            when  prescription.days_supply <=30 then '30'
            when  prescription.days_supply <=60 then '60'
            when  prescription.days_supply <=90 then '90'
            when  prescription.days_supply <=120 then '120'
            when  prescription.days_supply <=150 then '150'
            else '180'
        end as schedule_type,
        case 
            when  bill_header.dispense_date is null then '0'
            when  bill_header.dispense_date-prescription.start_date <=15 then '15'
            when  bill_header.dispense_date-prescription.start_date <=30 then '30'
            when  bill_header.dispense_date-prescription.start_date <=60 then '60'
            when  bill_header.dispense_date-prescription.start_date <=90 then '90'
            when  bill_header.dispense_date-prescription.start_date <=120 then '120'
            when  bill_header.dispense_date-prescription.start_date <=150 then '150'
            when  bill_header.dispense_date-prescription.start_date <=180 then '180'
            when  bill_header.dispense_date-prescription.start_date <=210 then '210'
            when  bill_header.dispense_date-prescription.start_date <=240 then '240'
            when  bill_header.dispense_date-prescription.start_date <=270 then '270'
            when  bill_header.dispense_date-prescription.start_date <=300 then '300'
            when  bill_header.dispense_date-prescription.start_date <=330 then '330'
            when  bill_header.dispense_date-prescription.start_date <=360 then '360'
            else '360+'
        end as days_since_first_fill_tier,
        bill_header.dispense_date-prescription.start_date as days_since_first_fill,
        case 
            when prescription.no_of_refill!=bill_header.fill_number 
            then bill_header.fill_number+1 
        end as next_fill_number,
    case 
    when prescription.doctor_id < 1 then 'Unknown'
    else coalesce(p.practice_group,'Unknown') 
    end as practice

FROM ips.bill_detail 
join ips.bill_header on  bill_header.tran_id = bill_detail.tran_id 
join ips.prescription on  prescription.tran_id = bill_header.rx_id 
join ips.patient_master on bill_header.patient_id = patient_master.id 
join ips.doctor_master on  prescription.doctor_id = doctor_master.srno 
join ips.drug_master on bill_header.drug_id = drug_master.drug_id 
left join ips.zip_master zm_pt ON patient_master.zip = zm_pt.srno
left join ips.responsible_party_master ON bill_header.account_id = responsible_party_master.srno 
left join ips.zip_master zm_dr ON doctor_master.zip = zm_dr.srno
left join {{ ref('dim_practice_map') }} p on p.practice=doctor_master.address
WHERE  prescription.office_id = 2

union all

select 
        p.patient_id, 
		p.drug_id, 
		p.doctor_id, 
		p.account_id,
        p.note, 
        p.rx_id as rxno, 
		p.rx_id::text||'-1'::text as refill_id, 
        p.med_type, 
        p.sig, 
        p.hold, 
        p.hold_note,
        p.hold_date,
        p.newfromold_label,
        p.start_date, 
        p.price as prescription_price, 
        p.qty, 
        p.drug_expire_date,
        p.rx_expire_date,
        p.tran_id, 
        p.price  as amount, 
        p.no_of_refill, 
        'N' as fill_status, 
        'N' as check_status, 
        'N' as dispense_flag, 
        null as dispense_date, 
        null as response_message, 
        p.created_date,
        p.changed_date, 
        p.office_id, 
        p.otc, 
        -1 as fill_number, 
        p.sig_english, 
        p.ips_bill, 
        null as delay_reason_code,
		
        case    
                when coalesce(p.last_tran_id,0) = 0  
                then false 
                else true
        end nfo_status,
        case    
                when p.rx_id ilike '%otc%' and charge_to_account ='Y'
                then 'Shipping'
                when p.rx_id ilike '%otc%' and charge_to_account ='N'
                then 'Samples'
                else 'Prescription'
        end as transaction_type,
        CASE    
                when p.rx_id ilike '%otc%' and charge_to_account ='Y' 
                then 0
                else bill_amount
        END as prescription_charge,
        CASE    when p.rx_id ilike '%otc%' and charge_to_account ='Y' 
                then bill_amount 
        END as shipping_charge,
        'actuals' as scenario, 
        coalesce(p.days_supply,1)::integer as days_supply,
        false as first_fill,
        false as fill_schedule_started,
        false as last_fill,
        case 
                when  p.days_supply <=15 then '15'
                when  p.days_supply <=30 then '30'
                when  p.days_supply <=60 then '60'
                when  p.days_supply <=90 then '90'
                when  p.days_supply <=120 then '120'
                when  p.days_supply <=150 then '150'
                else '180'
        end as schedule_type,
        '0' as days_since_first_fill_tier,
        0 as days_since_first_fill,
        0 as next_fill_number,
        case 
            when p.doctor_id < 1 then 'Unknown'
            else coalesce(pg.practice_group,'Unknown') 
        end as practice


from ips.prescription p
left join ips.bill_header h on  p.tran_id=h.rx_id
join ips.patient_master on p.patient_id = patient_master.id 
join ips.doctor_master on  p.doctor_id = doctor_master.srno 
join ips.drug_master on p.drug_id = drug_master.drug_id 
LEFT JOIN ips.facility_master ON patient_master.facility_id = facility_master.srno 
LEFT JOIN ips.zip_master zm_pt ON patient_master.zip = zm_pt.srno
LEFT JOIN ips.responsible_party_master ON p.account_id = responsible_party_master.srno 
LEFT JOIN ips.zip_master zm_dr ON doctor_master.zip = zm_dr.srno
left join {{ ref('dim_practice_map') }} pg on pg.practice=doctor_master.address
where h.tran_id is null
and p.rx_id not like 'otc%'
and p.office_id = 2
and p.active='Y'