select fs.product_id, product_name, category_name, brand_name, 
sum(ft.transaction_total) as total_revenue,
sum(fs.line_cost) as total_cogs,
sum(ft.transaction_total) - sum(fs.line_cost) as gross_profit
from {{ref('gold_fact_sale')}} fs inner join {{ref('gold_fact_completed_transaction')}} ft on fs.transaction_id = ft.transaction_id
inner join {{ref('gold_dim_product')}} dp on fs.product_id = dp.product_id
group by fs.product_id, product_name, category_name, brand_name