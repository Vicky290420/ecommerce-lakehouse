-- Staging layer: clean and standardize raw order data
SELECT
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    total_amount,
    LOWER(TRIM(status)) AS status,
    payment_method,
    shipping_city,
    batch_id,
    processed_at
FROM {{ source('raw', 'raw_orders') }}
WHERE order_id IS NOT NULL
  AND customer_id IS NOT NULL
