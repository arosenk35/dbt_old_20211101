select 
    h.master_rxno,
    min(h.master_rx_start_date)           as master_rx_start_date,
    max(h.last_rx_date)                   as master_rx_last_date,
    count(distinct p.rxno)                as nbr_renewals,
    count(distinct p.rxno||p.fill_number) as nbr_renewal_refills

from {{ ref('calc_refill_status') }}    h
join {{ ref('fact_prescription') }}     p on p.rxno=h.rxno
where h.master_rxno is not null

group by 1