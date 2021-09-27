{{
  config({
    "materialized": "table",
    "post-hook": [
        after_commit("create index  index_{{this.name}}_on_rxno on {{this.schema}}.{{this.name}} (rxno)"),
        after_commit("create index  index_{{this.name}}_on_id on {{this.schema}}.{{this.name}} (refill_id)")
        ]
  })
  }}
  SELECT distinct on (p.rx_id,bh.fill_number)
        p.rx_id       as rxno,
        p.rx_id::text||':'||fill_number::text as refill_id,
        bh.fill_number,
        dh.delivery_date + dh.delivery_time::timestamp as delivery_date,
        dh.tracking_number

FROM ips.prescription p 
      join ips.bill_header bh   on  p.tran_id =bh.rx_id 
      join ips.delivery_rx rx   on bh.walkin_tran_id=rx.fillcheck_id
      join ips.delivery_hdr dh  on dh.tran_id=rx.tran_id
    where p.rx_id not like 'otc%' and
        p.office_id = 2 and
        p.active='Y'