SELECT
c.date as status_date, 
h1.start_date,
h1.rx_expire_date,
rxno,
refill_status,
doctor_id,
patient_id,
drug_id,
days_supply,
date_part('year',h1.start_date ) = date_part('year',c.date ) as current_calendar

FROM  {{ ref('dim_calendar_monthly')}} c
	left join  {{ ref('calc_refill_status_history')}} h1 on (h1.rxno,h1.status_date)
	in(
		select h2.rxno, max(status_date) 
		from {{ ref('calc_refill_status_history')}} h2
		where h2.status_date <=c.date
		and h1.rxno=h2.rxno
	group by h2.rxno)
	where h1.refill_status='Open'

union all

SELECT
c.date as status_date, 
h1.start_date,
h1.rx_expire_date,
rxno,
refill_status,
doctor_id,
patient_id,
drug_id,
days_supply,
date_part('year',h1.start_date ) = date_part('year',c.date ) as current_calendar


from {{ ref('calc_refill_status_history')}} h1 

join{{ ref('dim_calendar_monthly')}} c on c.date =
(select max(d.date) from {{ ref('dim_calendar_monthly')}} d
where d.date<=h1.status_date)
where 
refill_status in ('Expired','Complete','Lost') 