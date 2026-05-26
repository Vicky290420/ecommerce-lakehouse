-- Staging layer: clean and standardize raw product data
SELECT
    product_id,
    TRIM(product_name) AS product_name,
    category,
    price,
    stock_quantity,
    TRIM(supplier) AS supplier,
    batch_id,
    processed_at
FROM {{ source('raw', 'raw_products') }}
WHERE product_id IS NOT NULL
