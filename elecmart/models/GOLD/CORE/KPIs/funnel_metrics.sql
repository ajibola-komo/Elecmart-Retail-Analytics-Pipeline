select date(session_end_time) as session_end_date,  to_char(session_end_time, 'YYYYMMDD')::INTEGER as session_end_date_id, count(session_id) as total_sessions,
       count(case when product_page_visited_flag = true then session_id end) as product_views,
       count(case when added_to_cart_flag = true then session_id end) as add_to_cart,
         count(case when purchased_flag = true then session_id end) as purchases
         from {{ ref('gold_fact_clickstream') }}
         group by 1,2