with calculate_line_discount as (
    select 
        fs.sale_id,
        fs.transaction_id,
        fs.session_id,
        fs.transaction_timestamp,
        fs.transaction_date_id,
        fs.product_id,
        ft.store_id,
        ft.customer_id,
        ft.sales_channel,
        ft.campaign_id,
        ft.promo_id,
        fs.quantity,
        fs.unit_cost,
        fs.unit_price,
        fs.line_cost,
        fs.line_total,
        round(fs.line_total / nullif(ft.transaction_subtotal, 0) 
        * coalesce(ft.transaction_discount_applied, 0), 2) as allocated_line_discount,
        ft.transaction_status as transaction_status
    from {{ ref('silver_fact_sale') }} fs
    join {{ ref('silver_fact_transaction') }} ft 
        on fs.transaction_id = ft.transaction_id
)

select 
    *,
    round(line_total - cld.allocated_line_discount, 2) as net_line_revenue
from calculate_line_discount cld 