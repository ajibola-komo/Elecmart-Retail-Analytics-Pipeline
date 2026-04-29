import duckdb as db
import numpy as np
import pandas as pd


def generate_customer_segments(conn):
    all_customers = conn.execute(
        '''SELECT customer_id, signup_date, location_id, loyalty_status, customer_persona FROM dim_customer
        order by signup_date desc'''
    ).df()

    def safe_sample(df):
        if len(df) == 0:
            return df 
        return df.sample(random_state=42)
    
    np.random.seed(42)

    all_customers["is_active"] = np.random.rand(len(all_customers)) < 0.8
    active_customers = all_customers[all_customers["is_active"]].copy()

    premium_customers = safe_sample(
        active_customers[active_customers['customer_persona'] == 'Tech Enthusiast'].sort_values('signup_date', ascending=False)
    )

    mid_customers = safe_sample(
        active_customers[active_customers['customer_persona'].isin(['Practical Buyer','Everyday Shopper'])].sort_values('signup_date', ascending=False)
    )

    basic_customers = safe_sample(
        active_customers[active_customers['customer_persona'].isin(['Bargain Hunter','Gift Shopper'])].sort_values('signup_date', ascending=False)
    )

    return {
        "all_customers": active_customers,
        "premium": premium_customers,
        "mid": mid_customers,
        "basic": basic_customers
    }