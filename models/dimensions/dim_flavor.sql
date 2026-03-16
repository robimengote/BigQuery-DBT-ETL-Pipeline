WITH flavors AS (
    SELECT 0 AS flavor_key, 'No Flavor' AS flavor_name
    UNION ALL
    SELECT 1, 'Cheese'
    UNION ALL
    SELECT 2, 'BBQ'
    UNION ALL
    SELECT 3, 'Sour Cream'
    UNION ALL
    SELECT 4, 'Plain'
    UNION ALL
    SELECT 5, 'Mango'
)

SELECT * FROM flavors