
SETTLEMENT_REPORT_SQL = """
     with cte as (
                select do2.restaurant_id, dr.restaurant_name, dt.date as settlement_date, 
                sum(count) as orders_count,
                sum(total_sum) as orders_total_sum,
                sum(bonus_payment) as orders_bonus_payment_sum,
                sum(bonus_grant) as order_bonus_granted_fee, 
                sum(total_sum) * 0.25 as order_processing_fee,
                sum(total_sum) - 0.25*sum(total_sum) - sum(bonus_payment) as reward


                from dds.fct_product_sales fps 
                join dds.dm_orders do2 on order_id = do2.id and do2.order_status  = 'CLOSED'
                join dds.dm_timestamps dt on do2.timestamp_id = dt.id 
                left join dds.dm_restaurants dr on do2.restaurant_id  = dr.id 
                
                group by do2.restaurant_id, dr.restaurant_name, dt.date
                )
                insert into cdm.dm_settlement_report (restaurant_id,restaurant_name,settlement_date,orders_count,orders_total_sum,
                orders_bonus_payment_sum,orders_bonus_granted_sum,order_processing_fee,restaurant_reward_sum)
                select *
                from cte
                on conflict (restaurant_id, settlement_date)
                do update set 
                restaurant_name = excluded.restaurant_name,
                orders_count = excluded.orders_count,
                orders_total_sum = excluded.orders_total_sum,
                orders_bonus_payment_sum = excluded.orders_bonus_payment_sum,
                orders_bonus_granted_sum = excluded.orders_bonus_granted_sum,
                order_processing_fee = excluded.order_processing_fee,
                restaurant_reward_sum = excluded.restaurant_reward_sum
    """

COURIER_LEDGER_SQL = """
with avg_rate as (
	select  courier_id, 
			year, month, 
			avg(rate) as rate_avg
			
	from dds.fct_delivery fd left outer join dds.dm_orders do2  on fd.order_id = do2.id 
						     left outer join dds.dm_timestamps dts on do2.timestamp_id  = dts.id 						     
	group by courier_id ,year ,month							
),
nd as (
select  fd.courier_id,dc."name"  as courier_name, 
		dts.year as settlement_year, dts.month as settlement_month, 
		count(distinct order_id) as orders_count,
		sum("sum") as  order_total_sum,		
		max(rate_avg) as rate_avg,		
		round( 0.25*sum("sum"),2) as order_processing_fee,
		round(sum( cdm.calc_oder_sum("sum",avg_rate.rate_avg)),2) as courier_order_sum,
		sum("tip_sum") as courier_tips_sum

	from dds.fct_delivery fd left outer join dds.dm_orders do2  on fd.order_id = do2.id 
						     left outer join dds.dm_timestamps dts on do2.timestamp_id  = dts.id 
						     left outer join dds.dm_couriers dc on fd.courier_id  = dc.id		
						     left outer join avg_rate on fd.courier_id = avg_rate.courier_id and dts.year = avg_rate.year and dts.month = avg_rate.month
	group by fd.courier_id ,dc.name,dts.year ,dts.month			
),
all_d as (
select *, (courier_order_sum + courier_tips_sum * 0.95) as courier_reward_sum from nd
)
insert into cdm.dm_courier_ledger (courier_id,courier_name,settlement_year,settlement_month,orders_count,order_total_sum,rate_avg,order_processing_fee,courier_order_sum,courier_tips_sum,courier_reward_sum) 
select * from all_d
on conflict on constraint  dm_courier_ledger_un_check do update set
	courier_name = EXCLUDED.courier_name,
	orders_count = EXCLUDED.orders_count,
	order_total_sum = EXCLUDED.order_total_sum,
	rate_avg = EXCLUDED.rate_avg,
	order_processing_fee = EXCLUDED.order_processing_fee,
	courier_order_sum = EXCLUDED.courier_order_sum,
	courier_tips_sum = EXCLUDED.courier_tips_sum,
	courier_reward_sum = EXCLUDED.courier_reward_sum

"""