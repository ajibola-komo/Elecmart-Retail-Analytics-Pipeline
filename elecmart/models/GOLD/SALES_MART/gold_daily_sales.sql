select ft.transaction_timestamp::DATE as transaction_date, 
ft.transaction_date_id,
ft.store_id, 
ft.sales_channel,
ft.transaction_status,
sum(ft.transaction_total) as total_sales,
sum(fs.line_cost) + sum(ft.transaction_discount_applied) as total_cost,
sum(ft.transaction_total) - (sum(fs.line_cost) + sum(ft.transaction_discount_applied)) as gross_profit,
(sum(ft.transaction_total) - (sum(fs.line_cost) + sum(ft.transaction_discount_applied))) / sum(ft.transaction_total) * 100 as profit_margin_percentage,
count(distinct ft.transaction_id) as total_transactions,
avg(ft.transaction_total) as average_transaction_value
from {{ref('silver_fact_sale')}} fs inner join {{ref('silver_fact_transaction')}} ft on fs.transaction_id = ft.transaction_id
inner join {{ref('silver_dim_date')}} dd on fs.transaction_date_id = dd.date_id
group by transaction_date, ft.transaction_date_id, store_id, sales_channel, transaction_status
order by transaction_date
