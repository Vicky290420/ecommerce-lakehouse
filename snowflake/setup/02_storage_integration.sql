-- NOTE: Replace YOUR_AWS_ACCOUNT_ID with your actual AWS account ID before running
USE DATABASE ecommerce_db;
USE SCHEMA raw;

CREATE OR REPLACE STORAGE INTEGRATION s3_ecommerce_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::524715311063:role/snowflake-s3-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://vky-lh26/silver/');

-- Get integration details - save STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
DESC STORAGE INTEGRATION s3_ecommerce_integration;
