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
        after_commit("create index  index_{{this.name}}_on_practice_id on {{this.schema}}.{{this.name}} (practice_id)")
      ]
  })
  }}

  SELECT 
        p.patient_id, 
        p.drug_id, 
        p.doctor_id, 
        p.account_id,
        p.note, 
        p.rx_id as rxno, 
        --- do not remove the sime colon it is being used everywhere and in all systems
        p.rx_id::text||':'||bh.fill_number::text as refill_id, 
        p.med_type, 
        p.sig, 
        p.hold, 
        p.hold_note,
        p.hold_date,
        p.start_date, 
        coalesce(p.price,(bh.patient_pay-bh.sales_tax_amount)) as prescription_price, 
        p.qty, 
        p.rx_expire_date,
        p.tran_id,  
        bh.patient_pay-bh.sales_tax_amount as amount, 
        p.no_of_refill, 
        bd.fill_status, 
        bd.check_status, 
        bh.dispense_flag, 
        bh.dispense_date, 
        bd.response_message, 
        p.created_date  as prescription_created_date,
        bd.created_date as bill_date,
        fp.delivery_date,
        fp.tracking_number,
        bh.fill_number, 
        p.ips_bill, 
        bd.delay_reason_code,
        case when coalesce(p.last_tran_id,0) = 0  
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
  --      CASE    when p.rx_id ilike '%otc%' and charge_to_account ='Y' 
  --              then 0
  --              else bh.patient_pay-bh.sales_tax_amount
  --     END as prescription_charge,
        CASE when p.rx_id ilike '%otc%' and charge_to_account ='Y' 
                then bh.patient_pay-bh.sales_tax_amount
        END as shipping_charge,
        'actuals' as scenario, 
        coalesce(p.days_supply,1)::integer as days_supply,
        bh.fill_number=0 as is_first_fill,
        bh.fill_number>0 as is_refill_renewal,
        bh.fill_number=1 as fill_schedule_started,
        no_of_refill=bh.fill_number as is_last_fill,
        case 
            when p.days_supply is null then '15'
            when p.days_supply <=15 then '15'
            when p.days_supply <=30 then '30'
            when p.days_supply <=60 then '60'
            when p.days_supply <=90 then '90'
            when p.days_supply <=120 then '120'
            when p.days_supply <=150 then '150'
            else '180'
        end as schedule_type,
        case 
            when bh.dispense_date is null then '0'
            when bh.dispense_date-p.start_date <=15 then '15'
            when bh.dispense_date-p.start_date <=30 then '30'
            when bh.dispense_date-p.start_date <=60 then '60'
            when bh.dispense_date-p.start_date <=90 then '90'
            when bh.dispense_date-p.start_date <=120 then '120'
            when bh.dispense_date-p.start_date <=150 then '150'
            when bh.dispense_date-p.start_date <=180 then '180'
            when bh.dispense_date-p.start_date <=210 then '210'
            when bh.dispense_date-p.start_date <=240 then '240'
            when bh.dispense_date-p.start_date <=270 then '270'
            when bh.dispense_date-p.start_date <=300 then '300'
            when bh.dispense_date-p.start_date <=330 then '330'
            when bh.dispense_date-p.start_date <=360 then '360'
            else '360+'
        end as days_since_first_fill_tier,
        bh.dispense_date-p.start_date as days_since_first_fill,
        case 
            when p.no_of_refill!=bh.fill_number 
            then bh.fill_number+1 
        end as next_fill_number,

    pg.practice_id,
    pgp.practice,

    (p.ips_bill='Y') as is_auto_fill,

    case 
            when p.sig_code ilike 'fou%' then 'Office Use'
            when p.sig_code ilike 'for%offic%' then 'Office Use'
        else 'Patient Use'
        end as prescription_type,
    case 
            when    sig ilike '%flavor%' and
                    split_part(split_part(sig,'(',2),')',1) ilike '%flavor%'
            then btrim(regexp_replace(regexp_replace( split_part(split_part(sig,'(',2),')',1),'flavored|flavors|flavor','','g'),'  ',' ','g'))
 
    end flavor,
    p.sig_code,
    case 
        when p.receive_through ='1' then 'Paper'
        when p.receive_through ='2' then 'Phone'
        when receive_through ='3' then 'Electronic'
        when receive_through ='4' then 'Fax'
        when receive_through ='5' then 'Transfer'
    end origin,
    bh.acquisition_cost

FROM ips.bill_detail bd
join ips.bill_header bh on bh.tran_id = bd.tran_id 
join ips.prescription p on p.tran_id =bh.rx_id 
join ips.patient_master on bh.patient_id = patient_master.id 
join ips.doctor_master on p.doctor_id = doctor_master.srno 
join ips.drug_master on bh.drug_id = drug_master.drug_id 
left join {{ ref('fact_delivery') }} fp on fp.refill_id=p.rx_id::text||':'||bh.fill_number::text 
left join ips.zip_master zm_pt ON patient_master.zip = zm_pt.srno
left join ips.responsible_party_master ON bh.account_id = responsible_party_master.srno 
left join ips.zip_master zm_dr ON doctor_master.zip = zm_dr.srno
left join {{ ref('dim_vet') }} pg on pg.doctor_id=p.doctor_id
left join {{ ref('dim_practice') }} pgp on pg.practice_id=pgp.practice_id
WHERE p.office_id = 2 and bh.patient_pay-bh.sales_tax_amount !=0

union all

---- this is to track in-progress transactions

select 
        p.patient_id, 
		p.drug_id, 
		p.doctor_id, 
		p.account_id,
        p.note, 
        p.rx_id as rxno, 
		p.rx_id::text||':'||'-1'::text as refill_id, 
        p.med_type, 
        p.sig, 
        p.hold, 
        p.hold_note,
        p.hold_date,
        p.start_date, 
        p.price as prescription_price, 
        p.qty, 
        p.rx_expire_date,
        p.tran_id, 
        p.price  as amount, 
        p.no_of_refill, 
        'N' as fill_status, 
        'N' as check_status, 
        'N' as dispense_flag, 
        null as dispense_date, 
        null as response_message, 
        p.created_date  as prescription_created_date,
        null as bill_date,
        null as delivery_date,
        null as tracking_number,
        -1 as fill_number, 
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
 --       CASE    
 --              when p.rx_id ilike '%otc%' and charge_to_account ='Y' 
 --               then 0
 --               else bill_amount
 --       END as prescription_charge,
        CASE    when p.rx_id ilike '%otc%' and charge_to_account ='Y' 
                then bill_amount 
        END as shipping_charge,
        'actuals' as scenario, 
        coalesce(p.days_supply,1)::integer as days_supply,
        bh.fill_number=0 as is_first_fill,
        bh.fill_number>0 as is_refill_renewal,
        false as fill_schedule_started,
        false as is_last_fill,
        case 
                when  p.days_supply is null then '15'
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
        pg.practice_id,
        pgp.practice,
        
        (p.ips_bill='Y') as is_auto_fill,
        case 
            when p.sig_code ilike 'fou%' then 'Office Use'
            when p.sig_code ilike 'for%offic%' then 'Office Use'
        else 'Standard'
        end as prescription_type,
        case 
            when    sig ilike '%flavor%' and
                    split_part(split_part(sig,'(',2),')',1) ilike '%flavor%'
            then btrim(regexp_replace(regexp_replace( split_part(split_part(sig,'(',2),')',1),'flavored|flavors|flavor','','g'),'  ',' ','g'))
        end flavor,
        sig_code,

        case 
        when p.receive_through ='1' then 'Paper'
        when p.receive_through ='2' then 'Phone'
        when p.receive_through ='3' then 'Electronic'
        when p.receive_through ='4' then 'Fax'
        when p.receive_through ='5' then 'Transfer'
        end origin,
        0::numeric as acquisition_cost


from ips.prescription p
left join ips.bill_header bh on  p.tran_id=bh.rx_id
join ips.patient_master on p.patient_id = patient_master.id 
join ips.doctor_master on  p.doctor_id = doctor_master.srno 
join ips.drug_master on p.drug_id = drug_master.drug_id
LEFT JOIN ips.facility_master ON patient_master.facility_id = facility_master.srno 
LEFT JOIN ips.zip_master zm_pt ON patient_master.zip = zm_pt.srno
LEFT JOIN ips.responsible_party_master ON p.account_id = responsible_party_master.srno 
LEFT JOIN ips.zip_master zm_dr ON doctor_master.zip = zm_dr.srno
left join {{ ref('dim_vet') }} pg on pg.doctor_id=p.doctor_id
left join {{ ref('dim_practice') }} pgp on pg.practice_id=pgp.practice_id
where bh.tran_id is null
and p.rx_id not like 'otc%'
and p.office_id = 2
and p.active='Y'