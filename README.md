# E-Commerce Data Lakehouse

An end-to-end, production-grade data pipeline that ingests raw logistics/e-commerce data, transforms it through a Medallion Architecture, and surfaces analytics-ready Fact and Dimension tables in Snowflake — fully orchestrated by Apache Airflow.

---

## Architecture

```
Watched Folder (CSV)
  ↓  Airflow FileSensor detects new files
S3 Bronze (raw CSV, batch_id partitioned)
  ↓  AWS Glue PySpark job
S3 Silver (Parquet, typed, cleaned)
  ↓  Snowflake External Stage (IAM zero-credential handshake)
Snowflake Raw Schema
  ↓  dbt (staging → marts)
Snowflake Gold (fact_orders, dim_customers, dim_products)
  ↓  Power BI
Analytics Dashboards

All steps orchestrated by Apache Airflow with retry logic and failure notifications
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, boto3, Apache Airflow FileSensor |
| Object Storage | Amazon S3 |
| Transformation | AWS Glue, Apache Spark (PySpark) |
| Data Warehouse | Snowflake |
| Data Modeling | dbt Core + dbt-snowflake |
| Orchestration | Apache Airflow (Docker) |
| BI | Power BI |
| Infrastructure | AWS IAM, Docker, Git |

---

## Key Design Decisions

### Batch-ID Partitioning for Idempotency

Every pipeline run creates a unique `batch_id=YYYY-MM-DD_HH-MM` folder in S3. This means:

- No file ever overwrites another
- Every run is fully isolated and replayable
- If a run fails, you can identify exactly when bad data entered the system
- The Glue job auto-detects the latest batch — no manual input required

### Zero-Credential S3-to-Snowflake Integration

Snowflake connects to S3 via IAM Role assumption with an External ID, not access keys. This means:

- No credentials stored in Snowflake or any config file
- AWS verifies identity via STS AssumeRole
- Rotating credentials doesn't break the pipeline

### Medallion Architecture

| Layer | Format | Purpose |
|---|---|---|
| Bronze | Raw CSV | Immutable source of truth — kept exactly as received |
| Silver | Parquet | Cleaned, typed, schema-enforced — optimized for compute |
| Gold | Snowflake tables | Business-modeled Fact/Dim tables for analytics |

### Why Parquet in Silver (not CSV)?

- 5-10x smaller file sizes (columnar compression)
- Column pruning — Snowflake reads only the columns it needs
- Schema embedded in file — no type guessing at load time
- Handles schema evolution gracefully

### Star Schema in Gold

The Gold layer models data into a star schema:

- `fact_orders` — one row per order, foreign keys to dimensions
- `dim_customers` — customer attributes with surrogate keys
- `dim_products` — product attributes with surrogate keys

Surrogate keys generated via `dbt_utils.generate_surrogate_key()` for referential integrity.

---

## Project Structure

```
ecommerce-lakehouse/
├── ingestion/
│   ├── scripts/
│   │   ├── generate_data.py       # Synthetic data generator
│   │   └── upload_to_s3.py        # Bronze layer upload
│   └── config/
│       └── config.yaml            # Bucket, region, data config
├── glue_jobs/
│   └── bronze_to_silver.py        # PySpark transformation job
├── ecommerce_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml        # Source definitions
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_products.sql
│   │   │   └── stg_orders.sql
│   │   └── marts/
│   │       ├── schema.yml         # dbt tests
│   │       ├── dim_customers.sql
│   │       ├── dim_products.sql
│   │       └── fact_orders.sql
│   ├── packages.yml               # dbt_utils dependency
│   └── dbt_project.yml
├── airflow/
│   ├── dags/
│   │   └── ecommerce_pipeline.py  # Main DAG
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   └── .env                       # AWS credentials (gitignored)
├── snowflake/
│   ├── setup/                     # Setup SQL scripts
│   └── load/                      # Data load SQL scripts
├── watched_folder/                # Drop CSV files here to trigger pipeline
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Pipeline DAG

```
wait_for_file
    ↓ (FileSensor — polls every 30s, 2hr timeout)
upload_to_s3
    ↓ (3 retries, exponential backoff: 2m → 4m → 8m)
trigger_glue_job
    ↓ (3 retries, exponential backoff: 5m → 10m → 20m)
wait_for_glue
    ↓ (polls every 60s, 1hr timeout, 2 retries)
run_dbt_models
    ↓ (3 retries, exponential backoff: 3m → 6m → 12m)
notify_success
```

Every task has:
- Task-level retry with exponential backoff
- `on_failure_callback` logging task name, attempt number, and exception
- Final exhaustion alert when all retries are consumed

---

## Data Quality

16 dbt tests covering:

- `unique` + `not_null` on all primary/surrogate keys
- `relationships` — foreign key integrity between fact and dimensions
- `not_null` on critical business columns (email, total_amount)

Run tests at any time:

```bash
cd ecommerce_dbt
dbt test
```

---

## Setup Guide

### Prerequisites

- Python 3.11+
- AWS account with S3 and Glue access
- Snowflake account (free trial works)
- Docker Desktop
- Git

### 1. Clone the repo

```bash
git clone https://github.com/your-username/ecommerce-lakehouse.git
cd ecommerce-lakehouse
```

### 2. Set up Python environment

```bash
python -m venv venv
source venv/Scripts/activate        # Windows (Git Bash)
source venv/bin/activate            # Mac/Linux
pip install -r requirements.txt
```

### 3. Configure AWS

```bash
aws configure
```

Update `ingestion/config/config.yaml` with your bucket name and region.

### 4. Generate synthetic data

```bash
python ingestion/scripts/generate_data.py
```

### 5. Set up Snowflake

Run the SQL scripts in order:

```
snowflake/setup/01_database_and_schemas.sql
snowflake/setup/02_storage_integration.sql
snowflake/setup/03_external_stage.sql
snowflake/load/01_create_raw_tables.sql
```

Follow the IAM role setup in `snowflake/README.md`.

### 6. Configure dbt

```bash
cd ecommerce_dbt
dbt deps
dbt debug     # verify connection
dbt build     # run all models and tests
```

### 7. Start Airflow

```bash
cd airflow
docker-compose up -d
```

Open http://localhost:8080 (admin / admin)

Add connections in Admin → Connections:
- `aws_default` — Amazon Web Services with your credentials
- `fs_default` — File System pointing to `/opt/airflow/watched_folder`

### 8. Trigger the pipeline

Drop CSV files into `watched_folder/`:

```bash
cp data/sample/orders.csv watched_folder/
cp data/sample/customers.csv watched_folder/
cp data/sample/products.csv watched_folder/
```

The FileSensor will detect them and kick off the full pipeline automatically.

---

## Data Model

### fact_orders

| Column | Type | Description |
|---|---|---|
| order_key | STRING | Surrogate key (MD5 hash) |
| order_id | STRING | Natural key from source |
| customer_key | STRING | FK to dim_customers |
| product_key | STRING | FK to dim_products |
| order_date | DATE | Order placement date |
| quantity | INTEGER | Units ordered |
| unit_price | FLOAT | Price per unit |
| total_amount | FLOAT | quantity × unit_price |
| status | STRING | Standardized lowercase status |
| payment_method | STRING | UPI, Credit Card, COD, etc. |

### dim_customers

| Column | Type | Description |
|---|---|---|
| customer_key | STRING | Surrogate key |
| customer_id | STRING | Natural key |
| email | STRING | Lowercased, trimmed |
| city | STRING | Customer city |
| signup_date | DATE | Account creation date |

### dim_products

| Column | Type | Description |
|---|---|---|
| product_key | STRING | Surrogate key |
| product_id | STRING | Natural key |
| product_name | STRING | Product display name |
| category | STRING | Product category |
| price | FLOAT | Clean numeric price (₹ stripped) |

---

## Intentional Data Challenges Handled

The raw data is deliberately messy to simulate real-world ingestion:

| Problem | Solution |
|---|---|
| Mixed date formats (`2024-01-15` vs `1/15/2024`) | `F.coalesce()` with two `to_date()` patterns |
| Inconsistent status case (`delivered` vs `DELIVERED`) | `F.lower(F.trim())` |
| Price column with currency symbol (`₹499.99`) | `F.regexp_replace()` before cast |
| Null values in shipping city and payment method | Filtered at fact table level |
| Schema evolution risk | Strict `StructType` enforcement on Glue read |

---

## Interview Talking Points

This project was designed to demonstrate specific production thinking:

**Idempotency** — batch_id partitioning ensures every run is isolated. Reprocessing any historical batch is a one-command operation.

**Zero-credential security** — Snowflake-to-S3 integration uses IAM role assumption with External ID. No long-lived credentials anywhere in the pipeline.

**Data quality as code** — 16 dbt tests run on every pipeline execution. Referential integrity between Fact and Dimension tables is enforced automatically.

**Observability** — Every Airflow task logs batch ID, attempt number, and exception on failure. All-retries-exhausted alerts flag tasks needing manual intervention.

**Schema enforcement** — PySpark reads with explicit `StructType`. Corrupt records fail fast at ingestion rather than silently corrupting downstream tables.

---

## Author

Vignesh Mahalingam
[linkedin.com/in/vignesh-mahalingam](https://linkedin.com/in/vignesh-mahalingam) | [github.com/your-username](https://github.com/your-username)
