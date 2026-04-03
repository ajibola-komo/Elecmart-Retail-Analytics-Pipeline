select sale_id::INTEGER as sale_id,
transaction_id::INTEGER as transaction_id,
session_id::INTEGER as session_id,
transaction_timestamp::timestamp_ntz as transaction_timestamp,
transaction_date_id::INTEGER as transaction_date_id,
product_id::INTEGER as product_id,
quantity::INTEGER as quantity,
cast(unit_cost as DECIMAL(10, 2)) as unit_cost,
cast(unit_price as DECIMAL(10, 2)) as unit_price,
cast(line_cost as DECIMAL(10, 2)) as line_cost,
cast(line_total as DECIMAL(10, 2)) as line_total
from {{source('bronze','fact_sale')}}