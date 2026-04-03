#src/config/paths - This documents includes all the file paths in this project

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DB_DIR = PROJECT_ROOT / "db"
DUCKDB_PATH = DB_DIR / "elec_mart.duckdb"



DATA_DIR = PROJECT_ROOT / "data"

EXPORT_DIR = DATA_DIR / "exports"
PARQUET_DIR = EXPORT_DIR / "parquet"
CSV_DIR = EXPORT_DIR / "csv"

CUSTOMERS_CSV_PATH = CSV_DIR / "dim_customer.csv"
STORES_CSV_PATH = CSV_DIR / "dim_store.csv"
BRANDS_CSV_PATH = CSV_DIR / "dim_brand.csv"
CATEGORIES_CSV_PATH = CSV_DIR / "dim_category.csv"
SUBCATEGORIES_CSV_PATH = CSV_DIR / "dim_subcategory.csv"
PRODUCTS_CSV_PATH = CSV_DIR / "dim_product.csv"
PROMOTIONS_CSV_PATH = CSV_DIR / "dim_promotion.csv"
CAMPAIGNS_CSV_PATH = CSV_DIR / "dim_campaign.csv"
CLICKSTREAMS_CSV_PATH = CSV_DIR / "fact_clickstream.csv"
TRANSACTIONS_CSV_PATH = CSV_DIR / "fact_transaction.csv"
LOCATION_CSV_PATH = CSV_DIR / "dim_location.csv"
SALES_CSV_PATH = CSV_DIR / "fact_sale.csv"
INVENTORY_CSV_PATH = CSV_DIR / "inventory.csv"
DATES_CSV_PATH = CSV_DIR / "dim_date.csv"

#---------------------------------- PARQUET FILE PATH ------------------------------------------
CUSTOMERS_PARQUET_PATH = PARQUET_DIR / "dim_customer.parquet"
STORES_PARQUET_PATH = PARQUET_DIR / "dim_store.parquet"
BRANDS_PARQUET_PATH = PARQUET_DIR / "dim_brand.parquet"
CATEGORIES_PARQUET_PATH = PARQUET_DIR / "dim_category.parquet"
SUBCATEGORIES_PARQUET_PATH = PARQUET_DIR / "dim_subcategory.parquet"
PRODUCTS_PARQUET_PATH = PARQUET_DIR / "dim_product.parquet"
PROMOTIONS_PARQUET_PATH = PARQUET_DIR / "dim_promotion.parquet"
CAMPAIGNS_PARQUET_PATH = PARQUET_DIR / "dim_campaign.parquet"
CLICKSTREAMS_PARQUET_PATH = PARQUET_DIR / "fact_clickstream.parquet"
TRANSACTIONS_PARQUET_PATH = PARQUET_DIR / "fact_transaction.parquet"
LOCATION_PARQUET_PATH = PARQUET_DIR / "dim_location.parquet"
SALES_PARQUET_PATH = PARQUET_DIR / "fact_sale.parquet"
INVENTORY_PARQUET_PATH = PARQUET_DIR / "inventory.parquet"
DATES_PARQUET_PATH = PARQUET_DIR / "dim_date.parquet"



#----------------------------------- DATA SCHEMAS PATH ----------------------------------------
SQL_DIR = PROJECT_ROOT / "sql"
DDL_DIR = SQL_DIR / "ddl"
SNOWFLAKE_DDL_DIR = SQL_DIR / "snowflake_ddl"
CUSTOMERS_DDL_PATH = DDL_DIR / "dim_customer.sql"
STORES_DDL_PATH = DDL_DIR / "dim_store.sql"
BRANDS_DDL_PATH = DDL_DIR / "dim_brand.sql"
CATEGORIES_DDL_PATH = DDL_DIR / "dim_category.sql"
SUBCATEGORIES_DDL_PATH = DDL_DIR / "dim_subcategory.sql"
PRODUCTS_DDL_PATH = DDL_DIR / "dim_product.sql"
PROMOTIONS_DDL_PATH = DDL_DIR / "dim_promotion.sql"
CAMPAIGNS_DDL_PATH = DDL_DIR / "dim_campaign.sql"
CLICKSTREAMS_DDL_PATH = DDL_DIR / "fact_clickstream.sql"
TRANSACTIONS_DDL_PATH = DDL_DIR / "fact_transaction.sql"
LOCATIONS_DDL_PATH = DDL_DIR / "dim_location.sql"
SALES_DDL_PATH = DDL_DIR / "fact_sale.sql"
INVENTORY_DDL_PATH = DDL_DIR / "inventory.sql"
DATE_DDL_PATH = DDL_DIR / "dim_date.sql"

#--------------------------------- CSV Files Path ---------------------------------------------
RAW_DIR = PROJECT_ROOT / "raw"
BRAND_RAW_PATH = RAW_DIR / "dim_brand.csv"
CATEGORY_RAW_PATH = RAW_DIR / "dim_category.csv"
SUBCATEGORY_RAW_PATH = RAW_DIR / "dim_subcategory.csv"
PRODUCT_RAW_PATH = RAW_DIR / "dim_product.csv"
CUSTOMERS_RAW_PATH = RAW_DIR/"dim_customer.csv"

#------------------------------- AWS S3 BUCKET FILE NAMES -------------------------------------
CUSTOMERS_S3_PARQUET_PATH = "dim_customer.parquet"
STORES_S3_PARQUET_PATH = "dim_store.parquet"
BRANDS_S3_PARQUET_PATH = "dim_brand.parquet"
CATEGORIES_S3_PARQUET_PATH = "dim_category.parquet"
SUBCATEGORIES_S3_PARQUET_PATH = "dim_subcategory.parquet"
PRODUCTS_S3_PARQUET_PATH = "dim_product.parquet"
PROMOTIONS_S3_PARQUET_PATH = "dim_promotion.parquet"
CAMPAIGNS_S3_PARQUET_PATH = "dim_campaign.parquet"
CLICKSTREAMS_S3_PARQUET_PATH = "fact_clickstream.parquet"
TRANSACTIONS_S3_PARQUET_PATH = "fact_transaction.parquet"
LOCATION_S3_PARQUET_PATH = "dim_location.parquet"
SALES_S3_PARQUET_PATH = "fact_sale.parquet"
INVENTORY_S3_PARQUET_PATH = "inventory.parquet"
DATES_S3_PARQUET_PATH = "dim_date.parquet"

S3_BUCKET_NAME = "elecmart-bucket"

TABLE_NAMES = ["dim_date", "dim_location", "dim_category", "dim_subcategory", "dim_brand","dim_product", "dim_customer", "dim_store", "dim_promotion",
               "dim_campaign","fact_clickstream","fact_transaction","fact_sale","inventory"]

S3_KEYS = [
    BRANDS_S3_PARQUET_PATH, 
    CAMPAIGNS_S3_PARQUET_PATH,
    CATEGORIES_S3_PARQUET_PATH,
    CUSTOMERS_S3_PARQUET_PATH,
    DATES_S3_PARQUET_PATH,
    CLICKSTREAMS_S3_PARQUET_PATH,
    SALES_S3_PARQUET_PATH,
    TRANSACTIONS_S3_PARQUET_PATH,
    INVENTORY_S3_PARQUET_PATH,
    LOCATION_S3_PARQUET_PATH,
    PRODUCTS_S3_PARQUET_PATH,
    PROMOTIONS_S3_PARQUET_PATH,
    STORES_S3_PARQUET_PATH,
    SUBCATEGORIES_S3_PARQUET_PATH,
]

s3_LOCAL_PATH = [
    BRANDS_PARQUET_PATH,
    CAMPAIGNS_PARQUET_PATH,
    CATEGORIES_PARQUET_PATH,
    CUSTOMERS_PARQUET_PATH,
    DATES_PARQUET_PATH,
    CLICKSTREAMS_PARQUET_PATH,
    SALES_PARQUET_PATH,
    TRANSACTIONS_PARQUET_PATH,
    INVENTORY_PARQUET_PATH,
    LOCATION_PARQUET_PATH,
    PRODUCTS_PARQUET_PATH,
    PROMOTIONS_PARQUET_PATH,
    STORES_PARQUET_PATH,
    SUBCATEGORIES_PARQUET_PATH
]

