select * from {{ref('silver_fact_clickstream')}} where
(product_page_visited_flag = False and added_to_cart_flag = True and purchased_flag = True)
or (product_page_visited_flag = False and added_to_cart_flag = False and purchased_flag = True)