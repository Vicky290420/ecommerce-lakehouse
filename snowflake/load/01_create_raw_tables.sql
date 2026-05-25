USE DATABASE ecommerce_db;
USE SCHEMA raw;
USE WAREHOUSE compute_wh;

-- Create customers table
CREATE OR REPLACE TABLE raw_customers (
  customer_id       STRING,
  first_name        STRING,
  last_name         STRING,
  email             STRING,
  phone_number      STRING,
  city              STRING,
  signup_date       DATE,
  last_login_date   DATE,
  batch_id          STRING,
  processed_at      TIMESTAMP_NTZ
);

-- Create products table
CREATE OR REPLACE TABLE raw_products (
  product_id        STRING,
  product_name      STRING,
  category          STRING,
  price             FLOAT,
  stock_quantity    INTEGER,
  supplier          STRING,
  batch_id          STRING,
  processed_at      TIMESTAMP_NTZ
);

-- Create orders table
CREATE OR REPLACE TABLE raw_orders (
  order_id          STRING,
  customer_id       STRING,
  product_id        STRING,
  order_date        DATE,
  quantity          INTEGER,
  unit_price        FLOAT,
  total_amount      FLOAT,
  status            STRING,
  payment_method    STRING,
  shipping_city     STRING,
  batch_id          STRING,
  processed_at      TIMESTAMP_NTZ
);
