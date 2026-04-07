select product_id, date_trunc('month', ft.transaction_timestamp)::DATE as transaction_date,
to_char(date_trunc('month', ft.transaction_timestamp), 'YYYYMMDD') as transaction_month_id,
sum(ft.transaction_total) as total_revenue,
sum(fs.line_cost) as total_cogs,
sum(ft.transaction_total) - sum(fs.line_cost) as gross_profit,
round((sum(ft.transaction_total) - sum(fs.line_cost)) / nullif(sum(ft.transaction_total), 0) * 100, 2) as profit_margin_percentage,
sum(fs.quantity) as total_units_sold
from {{ref('gold_fact_sale')}} fs inner join {{ref('gold_fact_completed_transaction')}} ft 
on fs.transaction_id = ft.transaction_id
inner join {{ref('gold_dim_date')}} dd on ft.transaction_timestamp::DATE = dd.date
group by product_id, transaction_date, transaction_month_id
order by transaction_date, product_id