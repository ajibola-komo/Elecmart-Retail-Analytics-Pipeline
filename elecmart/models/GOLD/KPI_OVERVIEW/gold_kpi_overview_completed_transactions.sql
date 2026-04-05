with get_revenue as (
select coalesce(sum(transaction_total), 0) as total_sales, coalesce(sum(transaction_cost), 0) as total_cost,
count(transaction_id) as total_transactions
 from {{ref('silver_fact_transaction')}} where transaction_status = 'Completed'
), 
final as 
(
select *, coalesce((total_sales - total_cost), 0) as gross_profit,
round(total_sales/nullif(total_transactions, 0), 2) as average_order_value
from get_revenue
)
select 
total_sales as total_completed_sales, 
total_cost as total_cost_for_completed_transactions, 
gross_profit as gross_profit_for_completed_transactions, 
round(gross_profit / nullif(total_sales, 0) * 100, 2) as profit_margin_for_completed_transactions,
total_transactions as total_completed_transactions, average_order_value as completed_average_order_value
 from final