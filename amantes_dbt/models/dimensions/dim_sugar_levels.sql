WITH sugar_levels AS (
    SELECT 0 AS sugar_key, 'No Sugar Level' AS sugar_level, NULL AS percentage
    UNION ALL
    SELECT 1, 'No Sugar', 0
    UNION ALL
    SELECT 2, 'Sugar 20%', 20
    UNION ALL
    SELECT 3, 'Sugar 50%', 50
    UNION ALL
    SELECT 4, 'Sugar 75%', 75
    UNION ALL
    SELECT 5, 'Sugar 100%', 100
)

SELECT * FROM sugar_levels