WITH spice_levels AS (
    SELECT 0 AS spice_key, 'No Spice Level' AS spice_level, NULL AS intensity
    UNION ALL
    SELECT 1, 'Mild (1/4)', 1
    UNION ALL
    SELECT 2, 'Regular (2/4)', 2
    UNION ALL
    SELECT 3, 'Spicy (3/4)', 3
    UNION ALL
    SELECT 4, 'Extra Spicy (4/4)', 4
)

SELECT * FROM spice_levels