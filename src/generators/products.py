import duckdb as db
from src.config.paths import (PRODUCT_RAW_PATH, PRODUCTS_DDL_PATH, PRODUCTS_CSV_PATH, PRODUCTS_PARQUET_PATH)

def generate_products(conn):
        create_db = PRODUCTS_DDL_PATH.read_text()
        conn.execute(create_db)

        conn.execute("DELETE FROM DIM_PRODUCT")

        conn.execute(f'''
                INSERT INTO DIM_PRODUCT (SELECT 
                     product_id, product_name, 
                     category_id, subcategory_id, 
                     brand_id, unit_cost, 
                     case 
                        when unit_cost < 50 then unit_cost * 1.25
                        when unit_cost < 150 then unit_cost * 1.20
                        when unit_cost < 600 then unit_cost * 1.15
                        else unit_cost * 1.08
                        end as unit_price,
                     warranty_years,
                     CASE
                        WHEN unit_price < 150 THEN 'Low'
                        WHEN unit_price < 200 THEN 'Entry Level'
                        WHEN unit_price < 600 THEN 'Mid Tier'
                        WHEN unit_price < 1500 THEN 'High End'
                    ELSE 'Flagship'
                    END AS product_segment
                    FROM READ_CSV_AUTO('{PRODUCT_RAW_PATH}'))
            ''')
        
        conn.execute(f'''
                COPY DIM_PRODUCT TO '{PRODUCTS_CSV_PATH}' (FORMAT CSV, HEADER true)
            ''')
        
        conn.execute(f'''
                    COPY DIM_PRODUCT TO '{PRODUCTS_PARQUET_PATH}' (FORMAT PARQUET)
''')