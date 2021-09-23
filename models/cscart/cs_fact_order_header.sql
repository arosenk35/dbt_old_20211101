{{
  config({
    "materialized": "table",
    "post-hook": [
      	after_commit("create index  index_{{this.name}}_on_ord_id on {{this.schema}}.{{this.name}} (order_id)"),
        after_commit("create index  index_{{this.name}}_on_status on {{this.schema}}.{{this.name}} (status)"),
        after_commit("create index  index_{{this.name}}_on_create_date on {{this.schema}}.{{this.name}} (order_date,status)")]
  })
}}
select
        o.order_id,
        coalesce(nullif(o.pet_data__user_id,'0'),nullif(o.user_id,'0'),'U'||o.order_id )|| ':' ||coalesce(o.pet_id,'')  as patient_id,
        coalesce(nullif(o.pet_data__user_id,'0'),nullif(o.user_id,'0'),'U'||o.order_id )                                as account_id,
        coalesce(nullif(o.vet_data__id,''),'U'||o.order_id)                                                           as doctor_id,
        o.issuer_id,
        nullif(btrim(o.notes),'')           as customer_notes,
        nullif(btrim(o.staff_notes),'')     as staff_notes,
        nullif(details,'')                  as details,
        o.tax_subtotal                      as tax_amount,
        o.paid_data__total                  as paid_amount,
        o.total                             as order_amount,
        is_patient_order='Y'                as is_patient_order,
        nullif(refill_order_id,'0') is not null  as is_refill_order,
        nullif(user_id,'0') is null         as is_guest_order,
        o.total - o.tax_subtotal -  o.shipping_cost  as gross_amount,
        o.subtotal_discount                 as discount,
        TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second' as order_date,
        o.status                            as status_code,
        o.shipping_cost                     as shipping_amount,
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
        end                                       as status,
        nullif(refill_order_id,'0')               as refill_order_id,

      is_order_due='y'                            as is_order_due ,
      is_parent_order = 'Y'                       as is_parent_order ,
      case 
          when  o.status in ('C','I')
          then null
          else    
          now()::date-(TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second')::date 
      end as days_open,
       case
            when  (TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second')::date-first_order_date::date  <=15 then '15'
            when  (TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second')::date-first_order_date::date  <=30 then '30'
            when  (TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second')::date-first_order_date::date  <=60 then '60'
            when  (TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second')::date-first_order_date::date  <=90 then '90'
            when  (TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second')::date-first_order_date::date  <=120 then '120'
            when  (TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second')::date-first_order_date::date  <=150 then '150'
            when  (TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second')::date-first_order_date::date  <=180 then '180'
            when  (TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second')::date-first_order_date::date  <=210 then '210' 
            when  (TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second')::date-first_order_date::date  <=240 then '240' 
            when  (TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second')::date-first_order_date::date  <=270 then '270'
            when  (TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second')::date-first_order_date::date  <=300 then '300'
            when  (TIMESTAMP 'epoch' + o.timestamp::numeric * INTERVAL '1 second')::date-first_order_date::date  <=330 then '330'
            else '360+'
        end as days_since_first_order_tier,
        s.shipping_method,
        s.shipping_class,
        sr.cold_shipping,
        case  when o.b_lastname like '%#%'
              then split_part(o.b_lastname,'#',2) 
              else nullif(regexp_replace(o.b_lastname,'[^0-9]+', '', 'g'),'') 
        end as po_nbr
from cscart.orders o
left join {{ ref('cs_segment_owner') }} so on so.account_id=o.user_id   
left join {{ ref('cs_fact_order_shipping') }}  s on s.order_id=o.order_id
left join {{ ref('cs_fact_order_shipping_req') }}  sr on sr.order_id=o.order_id