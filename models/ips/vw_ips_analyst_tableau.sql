{{
  config({
    "materialized": "view"
  })
}}
select
	--ids	
	fp.rxno ,
	fp.refill_id ,
	dd.drug_id,
	dp.practice_id,
	dp.organization_id,
	fp.patient_id,
	fp.account_id ,
	fqh.tracking_number,
	--dates	
	fp.dispense_date,
	fqh.created_date,
	fqh.checkout_date,
	fp.delivery_date,
	--fill specific
	fp.fill_number ,
	fp.days_supply ,
	fp.is_first_fill as flag_first_fill ,
	fp.is_last_fill as flag_last_fill ,
	fp.fill_schedule_started as flag_refill_1 ,
	--script specific	
	fp.no_of_refill ,
	fp.prescription_type ,
	--transaction specific
	fp.transaction_type ,
	fp.origin ,
	fp.is_auto_fill as flag_autofill ,
	--drug
	dd.drug_name,
	dd.master_drug,
	dd.drug_form,
	dd.strength,
	dd.strength_value,
	dd.qty_pack,
	dd.flavor,
	dd.unit_cost,
	dd.unit_price,
	dd.api_category,
	dd.controlled as flag_controlled_drug,
	dd.common as flag_common_drug,
	dd.active as flag_active_drug,
	dd.item_type,
	dd.is_complex_drug as flag_complex_drug,
	dd.bud_days,
	--practice
	fp.territory,
	dp.practice,
	dp.email as practice_email,
	dp.state as practice_state,
	dp.city as practice_city,
	dp.zip as practice_zip,
	dp.phone1 as practice_phone,
	dp.address as practice_address,
	dp.address2 as practice_address2,
	--owners
	dpa.email as pet_owner_email,
	do2.owner_name as pet_owner_name,
	--patient
	dpa.patient_name,
	dpa.species,
	--renewals
	dr.master_rxno,
	coalesce(dr.prescription_renewal,false) as flag_renewal,
	--refill
	drs.refill_status,
	--queue
	fqh.queue_status,
	--measures	
	fp.amount,
	dd.qty
from
	analytics_blue.fact_prescription fp
left join analytics_blue.dim_drug dd 
	on
	fp.drug_id = dd.drug_id
left join analytics_blue.dim_practice dp
on
	fp.practice_id = dp.practice_id
left join analytics_blue.der_renewals dr 
on
	fp.rxno = dr.rxno
left join analytics_blue.dim_patient dpa
on
	fp.patient_id = dpa.patient_id
left join analytics_blue.dim_owner do2 
on
	fp.account_id = do2.account_id
left join analytics_blue.der_refill_status drs 
on	fp.rxno = drs.rxno
left join analytics_blue.fact_queue_header fqh
on fp.walkin_tran_id = fqh.walkin_tran_id 
where
	extract (year from fp.dispense_date) >= extract(year from current_date)-2 