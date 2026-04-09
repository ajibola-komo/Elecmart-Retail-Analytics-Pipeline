WITH months AS (
    SELECT 
        DATE_TRUNC('month', DATEADD(month, seq4(), '2001-05-01')) AS month_start_date
    FROM TABLE(GENERATOR(ROWCOUNT => 600))
)
SELECT
    TO_CHAR(month_start_date, 'YYYYMM')::INT AS month_id,
    month_start_date,
    TO_CHAR(month_start_date, 'Month') AS month_name,
    EXTRACT(MONTH FROM month_start_date) AS month_number,
    EXTRACT(QUARTER FROM month_start_date) AS quarter,
    EXTRACT(YEAR FROM month_start_date) AS year
FROM months
WHERE month_start_date <= CURRENT_DATE
ORDER BY month_start_date