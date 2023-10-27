SELECT column_name, data_type
FROM `boxwood-magnet-396716.CaseStudyTarget.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'customers';

select min(order_purchase_timestamp) as minimum,
max(order_purchase_timestamp) as maximum
from `CaseStudyTarget.orders`;

select count(distinct c.customer_city) as Num_of_Cities ,count(distinct c.customer_state)
as Num_of_State
from `CaseStudyTarget.customers` c join `CaseStudyTarget.orders` o
on c.customer_id=o.customer_id;

select
sum(
case
when order_purchase_timestamp between '2016-09-04' and '2017-09-04' then 1
else 0
end
) as first_year,
sum(
case
when order_purchase_timestamp between '2017-09-05' and '2018-10-17' then 1
else 0
end
) as second_year
from `CaseStudyTarget.orders`;

with sidcte as(
select count(order_id) as no_of_orders ,extract(year from order_purchase_timestamp) as
year,
extract(month from order_purchase_timestamp) as month from `CaseStudyTarget.orders`
group by extract(month from order_purchase_timestamp),extract(year from
order_purchase_timestamp)
)
select * from sidcte
order by year,month;

select
sum(
case
when extract(hour from order_purchase_timestamp) between 0 and 6 then 1 else 0
end
) as Dawn,
sum(
case
when extract(hour from order_purchase_timestamp) between 7 and 12 then 1 else 0
end
) as Morning,
sum(
case
when extract(hour from order_purchase_timestamp) between 13 and 18 then 1 else 0
end
) as Afternoon,
sum(
case
when extract(hour from order_purchase_timestamp) between 19 and 23 then 1 else 0
end
) as Night
from `CaseStudyTarget.orders`;

select extract(year from order_purchase_timestamp) as year,
extract(month from order_purchase_timestamp) as month,
customer_state,
count(order_id) as cnt
from CaseStudyTarget.orders inner join CaseStudyTarget.customers using(customer_id)
group by 1,2,3
order by year,month;

with sidcte as(
select count(customer_id) as num_of_customer,customer_state from
`CaseStudyTarget.customers`
group by customer_state
)
select sidcte.num_of_customer,customer_state,
round(num_of_customer/(select count(customer_id) from `CaseStudyTarget.customers`)*100,2)
as distributed_percentage
from sidcte
order by sidcte.num_of_customer;

with sidcte as(
select sum(p.payment_value) as cost_of_orders,extract(year from order_purchase_timestamp)
as year
from `CaseStudyTarget.orders`o join `CaseStudyTarget.payments` p
on o.order_id=p.order_id
where extract(month from order_purchase_timestamp) between 1 and 8
group by extract(year from order_purchase_timestamp)
)
select *,round(((cost_of_orders-prev_count)/prev_count)*100,2) as increse_perct from(
select year,cost_of_orders,
lag(year) over(order by sidcte.year) as prev_year,
lead(cost_of_orders) over(order by cost_of_orders desc) as prev_count
from sidcte) t
where t.prev_count is not null
order by cost_of_orders desc;

select round(sum(ot.price),2) as total_price,
round(avg(ot.price),2) as avg_price,
c.customer_state
from `CaseStudyTarget.customers` c
join
`CaseStudyTarget.orders` o on c.customer_id=o.customer_id
join
`CaseStudyTarget.order_items` ot on ot.order_id=o.order_id
group by c.customer_state;

select round(sum(ot.freight_value),2) as total_freight_value,
round(avg(ot.freight_value),2) as avg_freight_value,
c.customer_state
from `CaseStudyTarget.customers` c
join
`CaseStudyTarget.orders` o on c.customer_id=o.customer_id
join
`CaseStudyTarget.order_items` ot on ot.order_id=o.order_id
group by c.customer_state;


select order_id,
date_diff(order_delivered_customer_date,order_purchase_timestamp,day) as time_to_deliver,
date_diff(order_estimated_delivery_date,order_delivered_customer_date,day) as
diff_estimated_delivery
from `CaseStudyTarget.orders`;

with sidcte as(
select
c.customer_state,
round(avg(ot.freight_value),2) as avg_freight_value
from `CaseStudyTarget.customers` c
join
`CaseStudyTarget.orders` o on c.customer_id=o.customer_id
join
`CaseStudyTarget.order_items` ot on ot.order_id=o.order_id
group by c.customer_state
),
siddcte as(
select *,dense_rank() over(order by avg_freight_value desc) as highest,
dense_rank() over(order by avg_freight_value) as lowest
from sidcte
)
select a.customer_state,a.avg_freight_value,a.highest,
b.customer_state,b.avg_freight_value,b.lowest
from siddcte a join siddcte b on a.highest=b.lowest
where a.highest<6 and b.lowest<6
order by highest;

with sidcte as(
select
c.customer_state,
round(avg(date_diff(o.order_delivered_customer_date,o.order_purchase_timestamp,day)),2) as
time_to_deliver
from `CaseStudyTarget.customers` c
join
`CaseStudyTarget.orders` o on c.customer_id=o.customer_id
group by c.customer_state
),
siddcte as(
select *,
dense_rank() over(order by time_to_deliver desc) as highest,
dense_rank() over(order by time_to_deliver) as lowest
from sidcte
)
select a.customer_state,a.time_to_deliver,a.highest,
b.customer_state,b.time_to_deliver,b.lowest
from siddcte a join siddcte b on a.highest=b.lowest
where a.highest<6 and b.lowest<6
order by highest;


with sidcte as(
select
c.customer_state,
round(avg(date_diff(o.order_delivered_customer_date,o.order_purchase_timestamp,day)),2) as
avg_deliver,
round(avg(date_diff(o.order_estimated_delivery_date,o.order_delivered_customer_date,day)),2
) as avg_estimate,
from `CaseStudyTarget.customers` c
join
`CaseStudyTarget.orders` o on c.customer_id=o.customer_id
group by c.customer_state
),
siddcte as(
select *,
dense_rank() over(order by difff desc) as rnk
from (
select *,round(avg_deliver-avg_estimate,2) as difff
from sidcte
)
)
select * from siddcte
where rnk<6
order by rnk;


select count(o.order_id) as num_of_orders,p.payment_type,
extract(year from o.order_purchase_timestamp) as year,
extract(month from o.order_purchase_timestamp) as month
from `CaseStudyTarget.orders` o join `CaseStudyTarget.payments` p using(order_id)
group by 2,3,4
order by year,month;

select count(order_id) as cnt, payment_installments
from `CaseStudyTarget.payments`
group by 2
having payment_installments>0