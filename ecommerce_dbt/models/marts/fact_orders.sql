-- Order fact table
{{ config(
    materialized='table'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT customer_key, customer_id FROM {{ ref('dim_customers') }}
),

products AS (
    SELECT product_key, product_id FROM {{ ref('dim_products') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} AS order_key,
    o.order_id,
    c.customer_key,
    p.product_key,
    o.order_date,
    o.quantity,
    o.unit_price,
    o.total_amount,
    o.status,
    o.payment_method,
    o.shipping_city,
    o.batch_id,
    o.processed_at
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN products p ON o.product_id = p.product_id
