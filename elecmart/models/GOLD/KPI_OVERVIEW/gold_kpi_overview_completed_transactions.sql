with get_revenue as (
select coalesce(sum(line_total), 0) as gross_revenue, 
coalesce(sum(allocated_line_discount), 0) as total_discount, 
coalesce(sum(line_cost), 0) as cogs,
count(distinct transaction_id) as total_transactions, 
sum(quantity) as units_sold
from {{ ref('gold_fact_sale') }} where transaction_status = 'Completed'
),  get_returned_metrics as (
select coalesce(sum(line_total), 0) as return_revenue,
coalesce(sum(line_cost), 0) as cogs_returned,
coalesce(sum(allocated_line_discount), 0) as total_discount_returned,
count(distinct transaction_id) as total_transactions_returned, 
sum(quantity) as units_returned,
 from {{ ref('gold_fact_sale') }} where transaction_status = 'Returned'
), final as (
select 
gross_revenue, 
cogs, 
total_discount,
total_transactions,
units_sold, 
return_revenue, 
cogs_returned,
total_discount_returned, 
total_transactions_returned, 
units_returned
from get_revenue cross join get_returned_metrics
)
select 
gross_revenue as gross_revenue,
units_returned, 
units_sold,
(gross_revenue - return_revenue - total_discount - total_discount_returned) as net_revenue, 
cogs,
(cogs - cogs_returned) as net_cogs,
(gross_revenue - return_revenue - total_discount - total_discount_returned) - (cogs - cogs_returned) as gross_profit, 
((gross_revenue - return_revenue - total_discount - total_discount_returned) - (cogs - cogs_returned)) / nullif((gross_revenue - return_revenue - total_discount - total_discount_returned), 0) * 100 as profit_margin,
total_transactions, 
units_sold, return_revenue, cogs_returned, total_transactions_returned, 
(total_transactions - total_transactions_returned) as net_transactions,
(gross_revenue - return_revenue - total_discount - total_discount_returned) / nullif(total_transactions, 0) as aov,
round(return_revenue / nullif(gross_revenue,0) * 100, 2) as return_rate_pct
 from final