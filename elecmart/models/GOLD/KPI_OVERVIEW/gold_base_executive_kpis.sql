with get_gross_metrics as (
select coalesce(sum(net_line_revenue), 0) as gross_sales_revenue,
count(distinct transaction_id) as total_orders
from {{ ref('gold_fact_sale') }} 
), 
get_net_metrics as 
(
select coalesce(sum(net_line_revenue), 0) as net_revenue,
count(distinct transaction_id) as total_transactions,
sum(quantity) as units_sold,
coalesce(sum(line_cost), 0) as cogs
from {{ ref('gold_fact_sale') }} where transaction_status = 'Completed'
), 
get_returned_metrics as (
select coalesce(sum(net_line_revenue), 0) as return_revenue,
sum(quantity) as units_returned
from {{ ref('gold_fact_sale') }} where transaction_status = 'Returned'
), 
get_clickstream_metrics as 
(
select count(session_id) as total_sessions,
count(case when purchased_flag = true then 1 end) as sessions_with_purchase,
count(case when added_to_cart_flag = true then 1 end) as sessions_with_add_to_cart,
count(case when product_page_view_flag = true then 1 end) as sessions_with_product_page_view,
count(case when added_to_cart_flag = true and purchased_flag = false then 1 end) as sessions_with_cart_abandonment
from {{ ref('gold_fact_clickstream') }}
)
final as (
select 
ggm.gross_sales_revenue, ggm.total_orders, gnm.net_revenue, grm.units_returned, 
gnm.total_transactions, gnm.units_sold, gnm.cogs, grm.return_revenue,
gcm.total_sessions, gcm.sessions_with_purchase, gcm.sessions_with_add_to_cart, gcm.sessions_with_product_page_view, 
gcm.sessions_with_cart_abandonment
from get_gross_metrics as ggm cross join get_net_metrics as gnm cross join get_returned_metrics as grm 
cross join get_clickstream_metrics as gcm
)
select
gross_sales_revenue as total_revenue,
total_orders,
net_revenue,
cogs,
(net_revenue - cogs) as gross_profit,
((net_revenue - cogs) * 1.0 / nullif(net_revenue, 0)) * 100 as profit_margin,
total_transactions,
units_sold,
return_revenue,
units_returned,
(net_revenue * 1.0 / nullif(total_transactions, 0)) as avg_order_value,
(return_revenue * 1.0 / nullif(gross_sales_revenue, 0) * 100) as return_rate_pct,
total_sessions,
(sessions_with_purchase * 1.0 / nullif(total_sessions, 0)) * 100 as conversion_rate_pct,
(sessions_with_add_to_cart * 1.0 / nullif(sessions_with_product_page_view, 0)) * 100 as add_to_cart_rate_pct,
(sessions_with_cart_abandonment * 1.0 / nullif(sessions_with_add_to_cart, 0)) * 100 as cart_abandonment_rate_pct
from final
