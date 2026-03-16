WITH order_types AS (
    SELECT 0 AS order_type_key, 'No Order Type' AS order_type
    UNION ALL SELECT 1, 'Dine In'
    UNION ALL SELECT 2, 'Takeaway'
)

SELECT * FROM order_types