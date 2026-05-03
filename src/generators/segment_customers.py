import duckdb as db
import numpy as np
import pandas as pd


def generate_customer_segments(conn):

    all_customers = conn.execute(
        '''
        SELECT customer_id,
               signup_date,
               location_id,
               loyalty_status,
               customer_persona
        FROM dim_customer
        '''
    ).df()

    all_customers["is_active"] = (
        np.random.rand(len(all_customers)) < 0.8
    )

    active_customers = all_customers[
        all_customers["is_active"]
    ].copy()

    premium_customers = active_customers[
        active_customers['customer_persona'] == 'Tech Enthusiast'
    ].sample(frac=1)

    mid_customers = active_customers[
        active_customers['customer_persona'].isin(
            ['Practical Buyer', 'Everyday Shopper']
        )
    ].sample(frac=1)

    basic_customers = active_customers[
        active_customers['customer_persona'].isin(
            ['Bargain Hunter', 'Gift Shopper']
        )
    ].sample(frac=1)

    return {
        "all_customers": active_customers,
        "premium": premium_customers,
        "mid": mid_customers,
        "basic": basic_customers
    }