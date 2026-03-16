WITH source AS (
    SELECT *
    FROM `pos-pipeline-airflow.pos_data.staging_fact_sales`
)

SELECT
    order_id,
    items,
    sub_category,
    category,
    flavor,
    variation,
    size,
    quantity,
    spice_level,
    sugar_level,
    total_order_amount,
    received_amount,
    payment_time,
    payment_type,
    order_type,
    source_file
FROM source