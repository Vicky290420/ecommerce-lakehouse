# Snowflake Setup

## Setup Order

Run scripts in this order:

1. `setup/01_database_and_schemas.sql` - Creates database, schemas, and warehouse
2. `setup/02_storage_integration.sql` - Creates S3 integration (requires AWS IAM setup)
3. `setup/03_external_stage.sql` - Creates external stage pointing to S3
4. `load/01_create_raw_tables.sql` - Creates raw tables
5. `load/02_load_data.sql` - Loads data from S3 parquet files

## AWS IAM Setup Required

Before running `02_storage_integration.sql`:
- Run `DESC STORAGE INTEGRATION` to get IAM user ARN and External ID
- Create IAM role `snowflake-s3-role` in AWS
- Configure trust policy with the External ID from Snowflake
- Attach S3 read policy for bucket `vky-lh26/silver/*`
