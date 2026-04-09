with source as (
    select inventory_id::INTEGER as inventory_id,
           product_id::INTEGER as product_id,
           store_id::INTEGER as store_id,
           snapshot_month::DATE as snapshot_month,
           cast(TO_CHAR(snapshot_month, 'YYYYMM') as int) AS snapshot_month_id,
           starting_stock::INTEGER as starting_stock,
           received_stock::INTEGER as received_stock,
           sold_units::INTEGER as sold_units,
           closing_stock::INTEGER as closing_stock,
           backorder_flag::BOOLEAN as backorder_flag,
           shrinkage_loss::INTEGER as shrinkage_loss
           from {{ source('bronze', 'inventory') }}
) select * from source