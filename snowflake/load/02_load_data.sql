USE DATABASE ecommerce_db;
USE SCHEMA raw;
USE WAREHOUSE compute_wh;

-- Load data from S3 parquet files
COPY INTO raw_customers
FROM @s3_silver_stage/customers/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO raw_products
FROM @s3_silver_stage/products/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO raw_orders
FROM @s3_silver_stage/orders/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Verify data loaded
SELECT 'customers' as table_name, COUNT(*) as row_count FROM raw_customers
UNION ALL
SELECT 'products', COUNT(*) FROM raw_products
UNION ALL
SELECT 'orders', COUNT(*) FROM raw_orders;
