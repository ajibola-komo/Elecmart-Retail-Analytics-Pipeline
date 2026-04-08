with get_complete_sales as (
select fs.product_id, dp.product_name, dp.category_name, dp.brand_name, 
sum(fs.net_line_revenue) as total_revenue,
sum(fs.line_cost) as total_cogs,
sum(fs.net_line_revenue) - sum(fs.line_cost) as gross_profit
from {{ref('gold_fact_sale')}} fs inner join {{ref('gold_dim_product')}} dp on fs.product_id = dp.product_id
where transaction_status = 'Completed'
group by fs.product_id, dp.product_name, dp.category_name, dp.brand_name
), get_returned_sales as (
select fs.product_id, dp.product_name, dp.category_name, dp.brand_name, 
sum(fs.net_line_revenue) as total_revenue,
sum(fs.line_cost) as total_cogs,
sum(fs.net_line_revenue) - sum(fs.line_cost) as gross_profit
from {{ref('gold_fact_sale')}} fs inner join {{ref('gold_dim_product')}} dp on fs.product_id = dp.product_id
where transaction_status = 'Returned'
group by fs.product_id, dp.product_name, dp.category_name, dp.brand_name)
select cs.product_id, cs.product_name, cs.category_name, cs.brand_name,
cs.total_revenue as total_revenue_completed,
cs.total_cogs as total_cogs_completed,
cs.gross_profit as gross_profit_completed,
rs.total_revenue as total_revenue_returned,
rs.total_cogs as total_cogs_returned,
rs.gross_profit as gross_profit_returned,
cs.total_revenue - rs.total_revenue as net_revenue,
cs.total_cogs - rs.total_cogs as net_cogs,
cs.gross_profit - rs.gross_profit as net_gross_profit
from get_complete_sales cs left join get_returned_sales rs on cs.product_id = rs.product_id
order by cs.product_id
