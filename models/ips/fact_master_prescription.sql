select h.master_rxno,
min(h.master_rx_start_date) as master_rx_start_date,
max(h.last_rx_date) as master_rx_last_date,
count(distinct p.rxno) as nbr_renewals,
count(distinct p.rxno||p.fill_number) as nbr_renewal_refills

from analytics_blue.calc_refill_status h
join analytics_blue.fact_prescription p on p.rxno=h.rxno

group by 1