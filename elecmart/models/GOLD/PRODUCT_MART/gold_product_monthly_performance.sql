with product_monthly_performance_completed_transactions as (
select product_id, date_trunc('month', fs.transaction_timestamp)::DATE as transaction_month,
to_char(date_trunc('month', fs.transaction_timestamp), 'YYYYMMDD') as transaction_month_id,
dp.product_name, dp.category_name, dp.brand_name,
sum(fs.transaction_total) as total_revenue,
sum(fs.line_cost) as total_cogs,
sum(fs.transaction_total) - sum(fs.line_cost) as gross_profit,
round((sum(fs.transaction_total) - sum(fs.line_cost)) / nullif(sum(fs.transaction_total), 0) * 100, 2) as profit_margin_percentage,
sum(fs.quantity) as total_units_sold
from {{ref('gold_fact_sale')}} fs inner join {{ref('gold_dim_date')}} dd on fs.transaction_timestamp::DATE = dd.date
where transaction_status = 'Completed'
group by product_id, transaction_month, transaction_month_id
), product_monthly_performance_returned_transactions as (
select product_id, date_trunc('month', fs.transaction_timestamp)::DATE as transaction_month,
to_char(date_trunc('month', fs.transaction_timestamp), 'YYYYMMDD') as transaction_month_id,
sum(fs.transaction_total) as total_revenue_returned,
sum(fs.line_cost) as total_cogs_returned,
sum(fs.transaction_total) - sum(fs.line_cost) as gross_profit_returned,
round((sum(fs.transaction_total) - sum(fs.line_cost)) / nullif(sum(fs.transaction_total), 0) * 100, 2) as profit_margin_percentage_returned,
sum(fs.quantity) as total_units_sold_returned
from {{ref('gold_fact_sale')}} fs inner join {{ref('gold_dim_date')}} dd on fs.transaction_timestamp::DATE = dd.date
where transaction_status = 'Returned'
group by product_id, transaction_month, transaction_month_id
)
select ct.product_id, ct.transaction_month, ct.transaction_month_id, ct.product_name, ct.category_name, ct.brand_name,
ct.total_revenue, ct.total_cogs, 
ct.gross_profit, ct.profit_margin_percentage, ct.total_units_sold, cr.total_revenue_returned, cr.total_cogs_returned, 
cr.gross_profit_returned, cr.profit_margin_percentage_returned, cr.total_units_sold_returned, 
ct.total_revenue - coalesce(cr.total_revenue_returned, 0) as net_revenue,
ct.total_cogs - coalesce(cr.total_cogs_returned, 0) as net_cogs,
ct.gross_profit - coalesce(cr.gross_profit_returned, 0) as net_gross_profit,
round((coalesce(cr.total_revenue_returned, 0) / nullif(ct.total_revenue, 0)) * 100, 2) as return_rate_revenue,
round((coalesce(cr.total_units_sold_returned, 0) / nullif(ct.total_units_sold, 0)) * 100, 2) as return_rate_units
from product_monthly_performance_completed_transactions ct left join product_monthly_performance_returned_transactions cr
on ct.product_id = cr.product_id and ct.transaction_month = cr.transaction_month
order by ct.product_id, ct.transaction_month