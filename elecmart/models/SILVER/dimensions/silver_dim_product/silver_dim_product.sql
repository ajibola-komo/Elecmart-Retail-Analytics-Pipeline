with source as (
    select product_id, initcap(product_name) as product_name, category_id::INTEGER as category_id, subcategory_id::INTEGER as subcategory_id,
    brand_id::INTEGER as brand_id, cast(unit_cost as DECIMAL(10,2)) as unit_cost, cast(unit_price as DECIMAL(10,2)) as unit_price,
    warranty_years::INTEGER as warranty_years, initcap(product_segment) as product_segment
    from {{source('bronze','dim_product')}}
), deduplicate as (
    select *, row_number() over(
        partition by product_id, product_name
        order by product_id
    ) as rn
    from source
), check_nulls as (
    select * from deduplicate where rn = 1 and 
    product_name is not null and
    unit_cost < unit_price and 
    unit_cost is not null and 
    unit_price is not null
) select product_id, product_name, category_id, subcategory_id, brand_id, unit_cost, unit_price, warranty_years, product_segment
from check_nulls