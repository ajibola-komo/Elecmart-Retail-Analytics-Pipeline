select * from {{ref('silver_fact_clickstream')}} where
campaign_id is not null and traffic_source != 'Campaign'