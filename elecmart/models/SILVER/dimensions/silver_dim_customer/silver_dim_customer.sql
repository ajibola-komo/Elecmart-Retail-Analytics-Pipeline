with source as (
    SELECT customer_id, lower(email_address) as email_address, initcap(first_name) as first_name, initcap(last_name) as last_name,
    lower(gender) as gender, initcap(customer_persona) as customer_persona, birth_date::DATE as birth_date,
    birth_year::INTEGER as birth_year, 
    datediff(year, birth_date, current_date()) as age,
    location_id::INTEGER as location_id, signup_date::DATE as signup_date, signup_date_id::INTEGER as signup_date_id,
    case when lower(signup_channel) = 'mobile app' then 'mobile'
    when lower(signup_channel) = 'online' then 'web'
    when lower(signup_channel) = 'in-store' then 'store'
    else lower(signup_channel)
    end as signup_channel, lower(loyalty_status) as loyalty_status, 
    cast(estimated_annual_income as DECIMAL(10, 2)) as estimated_annual_income,
    email_opt_in, sms_opt_in 
    from {{ source('bronze','dim_customer') }}
), 
deduplicate as (
    SELECT *, row_number() over(
        partition by email_address
        order by signup_date
    ) as rn from source
), deduplicated as ( select 
customer_id, email_address, first_name, last_name, gender, customer_persona, birth_date, birth_year, age,
location_id, signup_date, signup_date_id, signup_channel, loyalty_status, estimated_annual_income, email_opt_in, sms_opt_in
from deduplicate where rn = 1)
select customer_id, email_address, first_name, last_name, gender, customer_persona, birth_date, birth_year, age, case 
    when age between 18 and 24 then '18-24'
    when age between 25 and 34 then '25-34'
    when age between 35 and 44 then '35-44'
    when age between 45 and 54 then '45-54'
    else '55+'
    end as age_group,
location_id, signup_date, signup_date_id, signup_channel, loyalty_status, 
case when estimated_annual_income < 50000 then 'Low Income'
    when estimated_annual_income between 50000 and 100000 then 'Middle Income'
    else 'High Income'
    end as income_bracket,
email_opt_in, sms_opt_in
from deduplicated
