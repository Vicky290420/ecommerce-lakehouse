-- Customer dimension table
{{ config(
    materialized='table'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    city,
    signup_date,
    last_login_date,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM {{ ref('stg_customers') }}
