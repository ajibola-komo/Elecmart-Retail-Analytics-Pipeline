select s.location_id, store_name, store_type, store_size,
opening_date, opening_date_id, foot_traffic_index,
country, state_province, city, location_type
 from {{ref('silver_dim_store')}} s inner join {{ref('silver_dim_location')}} l
    on s.location_id = l.location_id