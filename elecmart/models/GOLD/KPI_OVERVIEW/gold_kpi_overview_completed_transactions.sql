with get_revenue as (
select coalesce(sum(transaction_total), 0) as total_sales, coalesce(sum(transaction_cost), 0) as total_cost,
count(transaction_id) as total_transactions, sum(items_count) as total_units_sold
 from {{ ref('gold_fact_completed_transaction') }}
), 
additional_metrics as 
(
select *, round(coalesce((total_sales - total_cost), 0), 2) as gross_profit,
round(total_sales/nullif(total_transactions, 0), 2) as average_order_value
from get_revenue
), get_returned_metrics as (
select coalesce(sum(transaction_total), 0) as total_revenue_returned, 
coalesce(sum(transaction_cost), 0) as total_cogs_returned,
count(transaction_id) as total_transactions_returned, sum(items_count) as total_units_returned,
round(coalesce((sum(transaction_total) - sum(transaction_cost)), 0), 2) as gross_profit_returned
 from {{ ref('gold_fact_returned_transaction') }}
), final as (
select total_sales, total_cost, gross_profit, average_order_value, total_transactions, total_units_sold, 
total_revenue_returned, total_cogs_returned, total_transactions_returned, total_units_returned, 
gross_profit_returned, round(gross_profit / nullif(total_sales, 0) * 100, 2) as profit_margin,
round(total_revenue_returned/nullif(total_transactions_returned,0),2) as average_order_value_returned
from additional_metrics cross join get_returned_metrics
)
select 
total_sales as total_revenue,
total_cost as total_cogs,
gross_profit as gross_profit, 
profit_margin,
total_transactions, 
total_units_sold, total_revenue_returned, total_cogs_returned, gross_profit_returned, total_transactions_returned, 
total_units_returned, (total_sales - total_revenue_returned) as net_revenue, 
(total_cost - total_cogs_returned) as net_cogs,
(total_transactions - total_transactions_returned) as net_transactions,
average_order_value as aov, average_order_value_returned as aov_returned,
(gross_profit - gross_profit_returned) as net_gross_profit,
round(total_revenue_returned / nullif(total_sales,0) * 100, 2) as return_rate_revenue,
round(total_transactions_returned / nullif(total_transactions,0) * 100, 2) as return_rate_transactions,
round((total_units_returned / nullif(total_units_sold,0)) * 100, 2) as return_rate_units
 from final