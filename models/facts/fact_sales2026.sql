{{ config(
    materialized='incremental',
    partition_by={
        "field": "payment_time",
        "data_type": "datetime",
        "granularity": "day"
    },
    cluster_by=["category", "sub_category"]    
) }}

WITH raw AS (
    SELECT *
    FROM `pos-pipeline-airflow.pos_data.staging_fact_sales`
    {% if is_incremental() %}
    WHERE payment_time > (SELECT MAX(payment_time) FROM {{ this }})
    {% endif %}

),

dim_product AS (SELECT * FROM {{ ref('dim_product') }}),	
dim_date AS (SELECT * FROM {{ ref('dim_date') }}),
dim_flavor AS (SELECT * FROM {{ ref('dim_flavor') }}),
dim_sugar AS (SELECT * FROM {{ ref('dim_sugar_levels') }}),
dim_spice AS (SELECT * FROM {{ ref('dim_spice_level') }}),
dim_payment AS (SELECT * FROM {{ ref('dim_payment_type') }}),
dim_order AS (SELECT * FROM {{ ref('dim_order_type') }})

SELECT
    -- ── Surrogate Keys ────────────────────────────────────
    CAST(raw.payment_time AS DATETIME)                               AS payment_time,
    CAST(FORMAT_DATE('%Y%m%d', DATE(raw.payment_time)) AS INT64)     AS date_key,
    COALESCE(dp.product_key, -1)                                     AS product_key,
    COALESCE(df.flavor_key, 0)                                       AS flavor_key,
    COALESCE(ds.sugar_key, 0)                                        AS sugar_key,
    COALESCE(dsp.spice_key, 0)                                       AS spice_key,
    COALESCE(dpt.payment_type_key, 0)                                AS payment_type_key,
    COALESCE(dot.order_type_key, 0)                                  AS order_type_key,

    -- ── Degenerate Dimensions ─────────────────────────────
    raw.order_id,
    raw.category,
    raw.sub_category,

    -- ── Measures ──────────────────────────────────────────
    raw.quantity,
    raw.total_order_amount,
    raw.received_amount

FROM raw

LEFT JOIN dim_product dp
    ON UPPER(TRIM(raw.items)) = UPPER(TRIM(dp.items))
    AND COALESCE(NULLIF(TRIM(raw.variation), ''), 'MATCH_BLANK') = COALESCE(NULLIF(TRIM(dp.variation), ''), 'MATCH_BLANK')
    AND COALESCE(NULLIF(TRIM(raw.size), ''), 'MATCH_BLANK') = COALESCE(NULLIF(TRIM(dp.size), ''), 'MATCH_BLANK')

LEFT JOIN dim_date dd
    ON CAST(FORMAT_DATE('%Y%m%d', DATE(raw.payment_time)) AS INT64) = dd.date_key

LEFT JOIN dim_flavor df
    ON COALESCE(raw.flavor, 'No Flavor') = df.flavor_name

LEFT JOIN dim_sugar ds
    ON COALESCE(raw.sugar_level, 'No Sugar Level') = ds.sugar_level

LEFT JOIN dim_spice dsp
    ON COALESCE(raw.spice_level, 'No Spice Level') = dsp.spice_level

LEFT JOIN dim_payment dpt
    ON raw.payment_type = dpt.payment_type

LEFT JOIN dim_order dot
    ON raw.order_type = dot.order_type