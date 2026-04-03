with source as (
    select promo_id::INTEGER as promo_id, initcap(promo_name) as promo_name, initcap(promo_type) as promo_type, initcap(discount_type) as discount_type,
    cast(discount_value as DECIMAL(10,2)) as discount_value, promo_start_date::TIMESTAMP_NTZ as promo_start_date,
    promo_start_date_id::INTEGER as promo_start_date_id, promo_end_date::TIMESTAMP_NTZ as promo_end_date, promo_end_date_id::INTEGER
    as promo_end_date_id, promo_duration::INTEGER as promo_duration, cast(is_active as BOOLEAN) as is_active,
    row_number() over(
        partition by promo_id
        order by promo_start_date
    ) as rn
    from {{source('bronze','dim_promotion')}}
), check_quality as (
    select * from source
    where rn = 1 and 
    promo_start_date is not null
) select 
    promo_id,
    promo_name,
    promo_type,
    discount_type,
    discount_value,
    promo_start_date,
    promo_start_date_id,
    promo_end_date,
    promo_end_date_id,
    is_active from check_quality
    order by promo_start_date
