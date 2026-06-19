{{ config(unique_key='date_id') }}

WITH date_series AS (
    SELECT full_date
    FROM UNNEST(
        SEQUENCE(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1' DAY)
    ) AS t(full_date)
)

SELECT
    CAST(DATE_FORMAT(full_date, '%Y%m%d') AS INTEGER) AS date_id,
    full_date,
    YEAR(full_date) AS year,
    MONTH(full_date) AS month,
    DAY(full_date) AS day,
    QUARTER(full_date) AS quarter,
    FORMAT_DATETIME(CAST(full_date AS TIMESTAMP), 'EEEE') AS day_of_week,
    FORMAT_DATETIME(CAST(full_date AS TIMESTAMP), 'MMMM') AS month_name,
    DAY_OF_WEEK(full_date) IN (6, 7) AS is_weekend,
    CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMP(6)) AS updated_at
FROM date_series
