-- Staging layer: clean and standardize raw customer data
SELECT
    customer_id,
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    LOWER(TRIM(email)) AS email,
    phone_number,
    city,
    signup_date,
    last_login_date,
    batch_id,
    processed_at
FROM {{ source('raw', 'raw_customers') }}
WHERE customer_id IS NOT NULL
