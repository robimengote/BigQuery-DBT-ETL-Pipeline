WITH date_spine AS (
    SELECT date
    FROM UNNEST(
        GENERATE_DATE_ARRAY('2026-01-01', '2027-12-31')
    ) AS date
),

holidays AS (
    SELECT holiday_date, holiday_name, holiday_type
    FROM {{ ref('ph_holidays') }}  -- dbt's way of referencing a seed
),

enriched AS (
    SELECT
        CAST(FORMAT_DATE('%Y%m%d', d.date) AS INT64) AS date_key,
        d.date AS full_date,
        EXTRACT(YEAR FROM d.date) AS year,
        EXTRACT(MONTH FROM d.date) AS month,
        FORMAT_DATE('%B', d.date) AS month_name,
        EXTRACT(DAY FROM d.date) AS day,
        FORMAT_DATE('%A', d.date) AS day_of_week,
        CASE WHEN FORMAT_DATE('%A', d.date) IN ('Saturday', 'Sunday')
            THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN h.holiday_date IS NOT NULL
            THEN TRUE ELSE FALSE END AS is_holiday,
        h.holiday_name,
        h.holiday_type
    FROM date_spine d
    LEFT JOIN holidays h ON d.date = h.holiday_date
)

SELECT * FROM enriched