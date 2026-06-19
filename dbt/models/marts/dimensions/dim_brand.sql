{{ config(unique_key='brand_id') }}

SELECT
    brand_id,
    ARBITRARY(brand_name) AS brand_name,
    CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMP(6)) AS updated_at
FROM {{ ref('stg_products') }}
WHERE brand_id IS NOT NULL
GROUP BY brand_id
