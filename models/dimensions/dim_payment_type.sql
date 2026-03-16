WITH payment_types AS (
    SELECT 0 AS payment_type_key, 'No Payment Type' AS payment_type
    UNION ALL SELECT 1, 'Free/Voucher/Discounted'
    UNION ALL SELECT 2, 'Credit / Debit'
    UNION ALL SELECT 3, 'Cash'
    UNION ALL SELECT 4, 'Gcash'
)

SELECT * FROM payment_types