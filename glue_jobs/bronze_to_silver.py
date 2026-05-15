import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
import boto3

# ── Job Init ───────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_BUCKET',
    'TARGET_BUCKET',
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_BUCKET = args['SOURCE_BUCKET']
TARGET_BUCKET = args['TARGET_BUCKET']


# ──────────────────────────────────────────────────────────────
# AUTO-DETECT LATEST BATCH ID FROM S3
# No manual input needed — always picks the most recent batch
# ──────────────────────────────────────────────────────────────
def get_latest_batch_id(bucket, prefix):
    """
    Lists all batch_id= folders under a given prefix
    and returns the most recent one by name (sorts lexicographically,
    which works because batch_id format is yyyy-MM-dd_HH-mm)
    """
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter='/'
    )

    if 'CommonPrefixes' not in response:
        raise Exception(f"No batch folders found at s3://{bucket}/{prefix}")

    # Extract batch_id values from folder names like raw/orders/batch_id=2024-01-15_10-23/
    batch_ids = []
    for obj in response['CommonPrefixes']:
        folder = obj['Prefix']
        match = re.search(r'batch_id=([^/]+)', folder)
        if match:
            batch_ids.append(match.group(1))

    if not batch_ids:
        raise Exception(f"No valid batch_id folders found at s3://{bucket}/{prefix}")

    latest = sorted(batch_ids)[-1]
    print(f"  Auto-detected latest batch_id: {latest}")
    return latest


# Use orders as the reference to find latest batch
BATCH_ID = get_latest_batch_id(SOURCE_BUCKET, "raw/orders/")
print(f"Processing Batch ID: {BATCH_ID}")


# ──────────────────────────────────────────────────────────────
# SCHEMAS
# ──────────────────────────────────────────────────────────────
CUSTOMERS_SCHEMA = StructType([
    StructField("CustomerID",    StringType(), False),
    StructField("FirstName",     StringType(), True),
    StructField("LastName",      StringType(), True),
    StructField("Email",         StringType(), True),
    StructField("PhoneNumber",   StringType(), True),
    StructField("City",          StringType(), True),
    StructField("SignupDate",    StringType(), True),
    StructField("LastLoginDate", StringType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("ProductID",     StringType(),  False),
    StructField("ProductName",   StringType(),  True),
    StructField("Category",      StringType(),  True),
    StructField("Price",         StringType(),  True),
    StructField("StockQuantity", IntegerType(), True),
    StructField("Supplier",      StringType(),  True),
])

ORDERS_SCHEMA = StructType([
    StructField("OrderID",       StringType(),  False),
    StructField("CustomerID",    StringType(),  True),
    StructField("ProductID",     StringType(),  True),
    StructField("OrderDate",     StringType(),  True),
    StructField("Quantity",      IntegerType(), True),
    StructField("UnitPrice",     DoubleType(),  True),
    StructField("TotalAmount",   DoubleType(),  True),
    StructField("Status",        StringType(),  True),
    StructField("PaymentMethod", StringType(),  True),
    StructField("ShippingCity",  StringType(),  True),
])


# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────
def clean_date_column(df, col_name):
    return df.withColumn(
        col_name,
        F.coalesce(
            F.to_date(F.col(col_name), 'yyyy-MM-dd'),
            F.to_date(F.col(col_name), 'M/d/yyyy'),
        )
    )

def to_snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def rename_columns_to_snake(df):
    for col_name in df.columns:
        snake = to_snake_case(col_name)
        if snake != col_name:
            df = df.withColumnRenamed(col_name, snake)
    return df


# ──────────────────────────────────────────────────────────────
# PROCESS CUSTOMERS
# ──────────────────────────────────────────────────────────────
print("Processing customers...")

customers_df = spark.read \
    .option("header", "true") \
    .schema(CUSTOMERS_SCHEMA) \
    .csv(f"s3://{SOURCE_BUCKET}/raw/customers/batch_id={BATCH_ID}/customers.csv")

customers_df = clean_date_column(customers_df, "SignupDate")
customers_df = clean_date_column(customers_df, "LastLoginDate")
customers_df = rename_columns_to_snake(customers_df)
customers_df = customers_df.filter(F.col("customer_id").isNotNull())
customers_df = customers_df \
    .withColumn("batch_id",     F.lit(BATCH_ID)) \
    .withColumn("processed_at", F.current_timestamp())

print(f"  Customers: {customers_df.count():,} rows")


# ──────────────────────────────────────────────────────────────
# PROCESS PRODUCTS
# ──────────────────────────────────────────────────────────────
print("Processing products...")

products_df = spark.read \
    .option("header", "true") \
    .schema(PRODUCTS_SCHEMA) \
    .csv(f"s3://{SOURCE_BUCKET}/raw/products/batch_id={BATCH_ID}/products.csv")

products_df = products_df.withColumn(
    "Price",
    F.regexp_replace(F.col("Price"), "[₹,]", "").cast(DoubleType())
)
products_df = rename_columns_to_snake(products_df)
products_df = products_df.filter(F.col("product_id").isNotNull())
products_df = products_df \
    .withColumn("batch_id",     F.lit(BATCH_ID)) \
    .withColumn("processed_at", F.current_timestamp())

print(f"  Products: {products_df.count():,} rows")


# ──────────────────────────────────────────────────────────────
# PROCESS ORDERS
# ──────────────────────────────────────────────────────────────
print("Processing orders...")

orders_df = spark.read \
    .option("header", "true") \
    .schema(ORDERS_SCHEMA) \
    .csv(f"s3://{SOURCE_BUCKET}/raw/orders/batch_id={BATCH_ID}/orders.csv")

orders_df = clean_date_column(orders_df, "OrderDate")
orders_df = orders_df.withColumn(
    "Status",
    F.lower(F.trim(F.col("Status")))
)
orders_df = rename_columns_to_snake(orders_df)
orders_df = orders_df.filter(
    F.col("order_id").isNotNull() &
    F.col("customer_id").isNotNull()
)
orders_df = orders_df \
    .withColumn("batch_id",     F.lit(BATCH_ID)) \
    .withColumn("processed_at", F.current_timestamp())

print(f"  Orders: {orders_df.count():,} rows")


# ──────────────────────────────────────────────────────────────
# WRITE TO SILVER
# ──────────────────────────────────────────────────────────────
print("Writing to Silver layer...")

silver_base = f"s3://{TARGET_BUCKET}/silver"

customers_df.write \
    .mode("overwrite") \
    .partitionBy("batch_id") \
    .parquet(f"{silver_base}/customers/")

products_df.write \
    .mode("overwrite") \
    .partitionBy("batch_id") \
    .parquet(f"{silver_base}/products/")

orders_df.write \
    .mode("overwrite") \
    .partitionBy("batch_id") \
    .parquet(f"{silver_base}/orders/")

print(f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Bronze → Silver Complete
  Batch ID  : {BATCH_ID}
  Target    : s3://{TARGET_BUCKET}/silver/
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")

job.commit()