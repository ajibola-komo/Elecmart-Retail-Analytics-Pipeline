import duckdb as db
import numpy as np
import pandas as pd
from src.generators.month_distribution import generate_online_month_distribution
from src.generators.segment_customers import generate_customer_segments
from src.config.paths import (CLICKSTREAMS_DDL_PATH, CLICKSTREAMS_CSV_PATH, CLICKSTREAMS_PARQUET_PATH)
from src.config.constants import (SESSION_START_ID, TRAFFIC_SOURCES, TRAFFIC_WEIGHTS,
                                  DEVICE_TYPES, DEVICE_WEIGHTS, SESSION_MINUTES, SESSION_WEIGHTS, PROB_OF_CAMPAIGN_LINKED,
                                  PROB_OF_PURCHASE, PROB_OF_PURCHASE_INTENTION, PROB_OF_CUSTOMER_SESSION, PROB_OF_REPEATED_SESSION)

def generate_clickstreams(conn,num_of_sessions_y1, num_of_sessions_y2):

    customer_segments = generate_customer_segments(conn)

    all_customers = customer_segments["all_customers"]
    premium_customers = customer_segments["premium"]
    mid_level_customers = customer_segments["mid"]
    basic_level_customers = customer_segments["basic"]

    premium_customers_subset = premium_customers.sample(frac=0.65, random_state=42)
    mid_level_customers_subset = mid_level_customers.sample(frac=0.65, random_state=42)
    basic_level_customers_subset = basic_level_customers.sample(frac=0.65, random_state=42)
    
    repeated_session = np.random.rand(num_of_sessions_y1 + num_of_sessions_y2) <= PROB_OF_REPEATED_SESSION

    campaigns_data = conn.execute("select campaign_id, campaign_start_date, campaign_end_date from dim_campaign").df()

    session_ids = np.arange(SESSION_START_ID, SESSION_START_ID + num_of_sessions_y1 + num_of_sessions_y2)

    online_distribution = generate_online_month_distribution(
    num_of_sessions_y1,
    num_of_sessions_y2
)

    session_start_times = np.array(
    online_distribution["y1"].tolist()
    + online_distribution["y2"].tolist()
)

    durations = np.random.choice(SESSION_MINUTES, p = SESSION_WEIGHTS, size= num_of_sessions_y1 + num_of_sessions_y2)

    session_end_times = session_start_times + pd.to_timedelta(durations, unit="m")

    device_types = np.random.choice(DEVICE_TYPES, p = DEVICE_WEIGHTS, size= num_of_sessions_y1 + num_of_sessions_y2)

    number_of_pages_viewed = np.where(durations <= 2, np.random.randint(1,4,size=num_of_sessions_y1 + num_of_sessions_y2),
                                    np.where((durations > 2) & (durations <= 5), np.random.randint(4,7, size = num_of_sessions_y1 + num_of_sessions_y2),
                                             np.random.randint(7,13, size= num_of_sessions_y1 + num_of_sessions_y2)))


    aov = np.full(num_of_sessions_y1 + num_of_sessions_y2, None, dtype=object)

    product_page_visited_flag = number_of_pages_viewed >= 4
    added_to_cart_flag = product_page_visited_flag & (np.random.rand(num_of_sessions_y1 + num_of_sessions_y2) <= PROB_OF_PURCHASE_INTENTION)
    purchased_flag = added_to_cart_flag & (np.random.rand(num_of_sessions_y1 + num_of_sessions_y2) <= PROB_OF_PURCHASE)
    is_customer_session = np.random.rand(num_of_sessions_y1 + num_of_sessions_y2) <= PROB_OF_CUSTOMER_SESSION
    customer_ids = np.full(num_of_sessions_y1 + num_of_sessions_y2, None, dtype=object)

    probability_of_campaign_linked = np.random.rand(num_of_sessions_y1 + num_of_sessions_y2) <= PROB_OF_CAMPAIGN_LINKED
    eligible_idx = np.where(probability_of_campaign_linked)[0]

    campaign_ids_for_sessions = np.full(num_of_sessions_y1 + num_of_sessions_y2, None, dtype = object)

    eligible_starts = np.array(session_start_times)[eligible_idx]

    camp_starts = campaigns_data['campaign_start_date'].to_numpy()
    camp_ends = campaigns_data['campaign_end_date'].to_numpy()
    camp_ids = campaigns_data['campaign_id'].to_numpy()

    valid_mask = (camp_starts[np.newaxis, :] <= eligible_starts[:, np.newaxis]) & \
             (camp_ends[np.newaxis, :]   >= eligible_starts[:, np.newaxis])

    for i, idx in enumerate(eligible_idx):
        valid_camps = camp_ids[valid_mask[i]]
        if valid_camps.size > 0:
            campaign_ids_for_sessions[idx] = np.random.choice(valid_camps)

    campaign_linked = pd.notnull(campaign_ids_for_sessions)
    linked_to_a_campaign_flag = np.where(campaign_linked, True, False)
    prob_of_premium_sessions = np.random.rand(len(is_customer_session)) <= 0.3 

    traffic_sources = np.full(num_of_sessions_y1 + num_of_sessions_y2, None, dtype=object)

    traffic_sources[linked_to_a_campaign_flag] = "Campaign"
    traffic_sources[~linked_to_a_campaign_flag] = np.random.choice(TRAFFIC_SOURCES, p = TRAFFIC_WEIGHTS, size=(~linked_to_a_campaign_flag).sum())
    
    eligible_premium_sessions = np.where(is_customer_session & purchased_flag & ~linked_to_a_campaign_flag & prob_of_premium_sessions)[0]
    eligible_starts = session_start_times[eligible_premium_sessions].to_numpy()

    premium_startup_dates = premium_customers['signup_date'].to_numpy()
    premium_ids = premium_customers['customer_id'].to_numpy()
    valid_mask = (premium_startup_dates[np.newaxis, :] <= eligible_starts[:, np.newaxis])

    premium_subset_ids = premium_customers_subset['customer_id'].to_numpy()
    premium_subset_signup_dates = premium_customers_subset['signup_date'].to_numpy()
    valid_mask_subset = (premium_subset_signup_dates[np.newaxis, :] <= eligible_starts[:, np.newaxis])

    for i, idx in enumerate(eligible_premium_sessions):
        if repeated_session[idx]:
            valid_ids = premium_subset_ids[valid_mask_subset[i]]
            if valid_ids.size > 0:
                customer_ids[idx] = np.random.choice(valid_ids)
                aov[idx] = 'High'
        else:
            valid_ids = premium_ids[valid_mask[i]]
            if valid_ids.size > 0:
                customer_ids[idx] = np.random.choice(valid_ids)
                aov[idx] = 'High'

    eligible_mid_level_sessions = np.where(is_customer_session & purchased_flag & ~linked_to_a_campaign_flag & ~prob_of_premium_sessions)[0]

    eligible_starts = session_start_times[eligible_mid_level_sessions].to_numpy()

    mid_startup_dates = mid_level_customers['signup_date'].to_numpy()
    mid_ids = mid_level_customers['customer_id'].to_numpy()
    valid_mask = (mid_startup_dates[np.newaxis, :] <= eligible_starts[:, np.newaxis])

    mid_subset_ids = mid_level_customers_subset['customer_id'].to_numpy()
    mid_subset_signup_dates = mid_level_customers_subset['signup_date'].to_numpy()
    valid_mask_subset = (mid_subset_signup_dates[np.newaxis, :] <= eligible_starts[:, np.newaxis])

    for i, idx in enumerate(eligible_mid_level_sessions):
        if repeated_session[idx]:
            valid_ids = mid_subset_ids[valid_mask_subset[i]]
            if valid_ids.size > 0:
                customer_ids[idx] = np.random.choice(valid_ids)
                aov[idx] = np.random.choice(['Low','Mid'], p = [0.4,0.6])
        else:
            valid_ids = mid_ids[valid_mask[i]]
            if valid_ids.size > 0:
                customer_ids[idx] = np.random.choice(valid_ids)
                aov[idx] = np.random.choice(['Low','Mid'], p = [0.4,0.6])

    eligible_basic_level_sessions = np.where(is_customer_session & purchased_flag & linked_to_a_campaign_flag)[0]

    eligible_starts = session_start_times[eligible_basic_level_sessions].to_numpy()

    basic_startup_dates = basic_level_customers['signup_date'].to_numpy()
    basic_ids = basic_level_customers['customer_id'].to_numpy()
    valid_mask = (basic_startup_dates[np.newaxis, :] <= eligible_starts[:, np.newaxis])

    basic_subset_ids = basic_level_customers_subset['customer_id'].to_numpy()
    basic_subset_signup_dates = basic_level_customers_subset['signup_date'].to_numpy()
    valid_mask_subset = (basic_subset_signup_dates[np.newaxis, :] <= eligible_starts[:, np.newaxis])

    for i, idx in enumerate(eligible_basic_level_sessions):
        if repeated_session[idx]:
            valid_ids = basic_subset_ids[valid_mask_subset[i]]
            if valid_ids.size > 0:
                customer_ids[idx] = np.random.choice(valid_ids)
                aov[idx] = 'Low'
        else:
            valid_ids = basic_ids[valid_mask[i]]
            if valid_ids.size > 0:
                customer_ids[idx] = np.random.choice(valid_ids)
                aov[idx] = 'Low'
    
    eligible_customer_sessions = np.where(np.logical_and(is_customer_session, customer_ids == None))[0]
    eligible_starts = session_start_times[eligible_customer_sessions].to_numpy()

    customer_signup_dates = all_customers['signup_date'].to_numpy()
    customer_ids_array = all_customers['customer_id'].to_numpy()

    sorted_idx = np.argsort(customer_signup_dates)
    sorted_dates = customer_signup_dates[sorted_idx]
    sorted_ids = customer_ids_array[sorted_idx]

    for i, idx in enumerate(eligible_customer_sessions):
        start_time = eligible_starts[i]

    # all customers with signup <= start_time
        cutoff = np.searchsorted(sorted_dates, start_time, side='right')
        valid_ids = sorted_ids[:cutoff]

        if valid_ids.size > 0:
            customer_ids[idx] = np.random.choice(valid_ids)
            aov[idx] = np.random.choice(['Low', 'Mid', 'High'], p=[0.5, 0.3, 0.2])
    

    remaining_transactions = pd.isna(aov)
    aov[remaining_transactions] = np.random.choice(['Low', 'Mid', 'High'], p=[0.5, 0.3, 0.2], size=remaining_transactions.sum())

    df_raw = pd.DataFrame({
        'session_id': session_ids,
        'customer_id': customer_ids,
        'session_start_time': session_start_times,
        'session_start_date_id': pd.to_datetime(session_start_times).strftime('%Y%m%d').astype(int),
        'session_end_time': session_end_times,
        'session_end_date_id': pd.to_datetime(session_end_times).strftime('%Y%m%d').astype(int),
        'device_type': device_types,
        'number_of_pages_viewed': number_of_pages_viewed,
        'product_page_visited_flag': product_page_visited_flag,
        'added_to_cart_flag': added_to_cart_flag,
        'purchased_flag': purchased_flag,
        'traffic_source': traffic_sources,
        'linked_to_a_campaign_flag': linked_to_a_campaign_flag,
        'campaign_id': campaign_ids_for_sessions,
        'aov_category': aov
        })
        
    conn.execute(CLICKSTREAMS_DDL_PATH.read_text())

    conn.execute(f"DELETE FROM FACT_CLICKSTREAM")
        
    conn.register("df_raw", df_raw)

    conn.execute("INSERT INTO fact_clickstream SELECT * FROM df_raw")
    
    conn.execute(f"COPY FACT_CLICKSTREAM TO '{CLICKSTREAMS_CSV_PATH}' (FORMAT CSV, HEADER true)")
    
    conn.execute(f'''COPY FACT_CLICKSTREAM TO '{CLICKSTREAMS_PARQUET_PATH}' (FORMAT PARQUET)''')