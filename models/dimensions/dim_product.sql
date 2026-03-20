WITH products AS (
    SELECT *
    FROM {{ ref('products_seed') }}
),

final AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY category, sub_category, items, variation, size
        ) AS product_key,
        items,
        sub_category,
        category,
        variation,
        size
    FROM products
)

SELECT * FROM final