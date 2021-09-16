select
    pth.tran_id as price_plan_id, 
    pth.description, 
    pth.cost_type, 
    pth.minimum_price, 
    pth.allow_customer_discount,
    pth.allow_special_prices, 
    pth.round_up, 
    pth.round_to_number, 
    pth.unit_dose_markup, 
    pth.unit_dose_markup_type, 
    pth.allow_override_at_fill, 
    pth.active, 
    pth.created_date, 
    pth.changed_date,
    pth.calculation_base_flag,
    pth.flat_flag
	
from 
ips.price_template_hdr pth 