select
 ptd.tran_id as price_plan_id, 
 ptd.sr_id as line_id, 
 ptd.amount_from, 
 ptd.amount_to,
 ptd.markup_factor, 
 ptd.add_fee, 
 ptd.created_date,  
 ptd.changed_date, 
 ptd.markup_flat
from ips.price_template_dtl ptd 