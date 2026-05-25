USE DATABASE ecommerce_db;
USE SCHEMA raw;

CREATE OR REPLACE STAGE s3_silver_stage
  STORAGE_INTEGRATION = s3_ecommerce_integration
  URL = 's3://vky-lh26/silver/'
  FILE_FORMAT = (TYPE = PARQUET);

-- Test the stage
LIST @s3_silver_stage/orders/;
