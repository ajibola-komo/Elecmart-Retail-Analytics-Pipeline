with source as (
select transaction_id::INTEGER as transaction_id,
transaction_timestamp::timestamp_ntz as transaction_timestamp,
transaction_date_id::INTEGER as transaction_date_id,
customer_id::INTEGER as customer_id,
store_id::INTEGER as store_id,
case when sales_channel = 'In-Store' then 'store' else lower(sales_channel) end as sales_channel, 
session_id::INTEGER as session_id,
promo_id::INTEGER as promo_id, 
campaign_id::INTEGER as campaign_id,
cast(transaction_subtotal as decimal(10,2)) as transaction_subtotal,
cast(transaction_discount_applied as decimal(10,2)) as transaction_discount_applied,
cast(transaction_total as decimal(10,2)) as transaction_total,
cast(transaction_cost as decimal(10,2)) as transaction_cost,
items_count::INTEGER as items_count, payment_type as payment_type,
transaction_status from {{source('bronze','fact_transaction')}}
) select * from source