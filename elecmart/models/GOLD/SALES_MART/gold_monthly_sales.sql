select 
date_trunc('month',fs.transaction_timestamp) as transaction_month, 
cast(to_char(date_trunc('month',fs.transaction_timestamp), 'YYYYMM') as int) as transaction_month_id,
fs.store_id,
sum(case when fs.transaction_status = 'Completed' then fs.net_line_revenue else 0 end) as total_revenue,
sum(case when fs.transaction_status = 'Completed' then fs.line_cost else 0 end) as total_cogs,
sum(case when fs.transaction_status = 'Completed' then fs.net_line_revenue - fs.line_cost else 0 end) as gross_profit,
(sum(case when fs.transaction_status = 'Completed' then fs.net_line_revenue else 0 end) - sum(case when fs.transaction_status = 'Completed' then fs.line_cost else 0 end)) / nullif(sum(case when fs.transaction_status = 'Completed' then fs.net_line_revenue else 0 end), 0) * 100 as profit_margin_percentage,
count(distinct case when fs.transaction_status = 'Completed' then fs.transaction_id else null end) as total_transactions,
avg(case when fs.transaction_status = 'Completed' then fs.net_line_revenue else null end) as average_transaction_value,
sum(case when fs.transaction_status = 'Completed' then fs.quantity else 0 end) as total_units_sold,
sum(case when fs.transaction_status = 'Returned' then fs.net_line_revenue else 0 end) as total_revenue_returned,
sum(case when fs.transaction_status = 'Returned' then fs.line_cost else 0 end) as total_cogs_returned,
sum(case when fs.transaction_status = 'Returned' then fs.net_line_revenue - fs.line_cost else 0 end) as gross_profit_returned,
round((sum(case when fs.transaction_status = 'Returned' then fs.net_line_revenue else 0 end) / nullif(sum(case when fs.transaction_status = 'Completed' then fs.net_line_revenue else 0 end), 0)) * 100, 2) as return_rate_percentage,
round((sum(case when fs.transaction_status = 'Returned' then fs.quantity else 0 end) / nullif(sum(case when fs.transaction_status = 'Completed' then fs.quantity else 0 end), 0)) * 100, 2) as return_rate_units_percentage
from {{ref('gold_fact_sale')}} fs
group by transaction_month, transaction_month_id, store_id
order by transaction_month