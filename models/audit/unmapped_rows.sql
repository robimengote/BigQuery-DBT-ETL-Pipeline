{{ config(materialized='view') }}

WITH staging_sales AS (
    SELECT * FROM {{ source('staging_fact_sales', 'staging_fact_sales') }}
),

dim_payment AS (
    SELECT * FROM {{ ref('dim_payment_type') }}
),

dim_product AS (
    SELECT * FROM {{ ref('dim_product') }}
),

dim_order AS (
    SELECT * FROM {{ ref('dim_order_type') }}
)

SELECT 
    s.order_id,
    s.payment_time,
    s.payment_type,  -- <-- This is the gold! The actual typo.
    s.total_order_amount,
    s.received_amount
FROM staging_sales s
LEFT JOIN dim_payment d ON s.payment_type = d.payment_type
LEFT JOIN dim_product dp ON s.items = dp.items
LEFT JOIN dim_order do ON s.order_type = do.order_type
-- The magic filter: Only show me the rows that FAILED the join
WHERE d.payment_type_key IS NULL OR do.order_type_key = 0 OR d.payment_type_key = 0