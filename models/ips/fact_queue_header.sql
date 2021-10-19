{{
  config({
    "materialized": "table",
    "post-hook": [
        after_commit("create index  index_{{this.name}}_on_rxno on {{this.schema}}.{{this.name}} (walkin_tran_id)")
        ]
  })
  }}
select  
    tran_id as walkin_tran_id,
    tran_type,
    signature_flag,
    checkout_tran_id,
    (checkin_date+checkin_time)::timestamp as  checkin_date,
    (checkout_date+checkout_time)::timestamp as checkout_date,
    (fill_date+fill_time)::timestamp as fill_date,
    (print_date+print_time)::timestamp as printed_date,
    (pickup_date+pickup_time)::timestamp as pickup_date,
    created_date,
    tracking_number,
    company_name,
    queue_status
from ips.walkin_header
where office_id=2