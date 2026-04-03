select store_id::INTEGER as store_id,
initcap(store_name) as store_name,
location_id::INTEGER as location_id,
initcap(store_type) as store_type,
store_size::INTEGER as store_size,
opening_date::DATE as opening_date,
opening_date_id::INTEGER as opening_date_id,
foot_traffic_index::INTEGER as foot_traffic_index
from {{source('bronze','dim_store')}}