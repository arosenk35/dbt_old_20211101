{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_ord_id on {{this.schema}}.{{this.name}} (order_id)"),
        after_commit("create index  index_{{this.name}}_on_status on {{this.schema}}.{{this.name}} (status)"),
        after_commit("create index  index_{{this.name}}_on_create_date on {{this.schema}}.{{this.name}} (created_date)")]
  })
}}
select
        o.order_id,
        coalesce(nullif(o.pet_data__user_id,'0'),o.user_id)|| coalesce(o.pet_id,'')     as patient_id,
        coalesce(nullif(pet_data__user_id,'0'),o.user_id) 	                            as account_id,
        o.user_id,
        o.issuer_id,
        o.doctor_data__user_id      as doctor_id,
        nullif(o.notes,'')          as notes,
        nullif(o.staff_notes,'')    as staff_notes,
        o.tax_subtotal              as tax_amount,
        o.paid_data__total          as paid_amount,
        o.total                     as order_amount ,
        o.subtotal                  as gross_amount,
        o.subtotal_discount         as discount,
        TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second' as created_date,
        o.status                    as status_code,
        o.shipping_cost             as shipping_amount,
        case o.status
            when 'P' then 'Awaiting Fulfillment'
            when 'C' then 'Complete'
            when 'O' then 'Open'
            when 'F' then 'Failed'
            when 'D' then 'Declined'
            when 'B' then 'Backordered'
            when 'I' then 'Cancelled'
            when 'Y' then 'Awaiting Call'
            when 'A' then 'Confirm Order Details'
            when 'E' then 'Fraud Checking'
            when 'G' then 'Processed'
            when 'H' then 'Pending Review'
            when 'J' then 'Processed (PK)'
            when 'K' then 'New Patient Order'
            when 'L' then 'Pending VET Approval'
        end as status,
        nullif(refill_order_id,'0')               as refill_order_id,
        (o.doctor_data__user_id is null)          as direct_purchase,
        (nullif(refill_order_id,'0') is not null) as refill_order,

is_order_due,
is_parent_order,
is_patient_order

from cscart.orders o