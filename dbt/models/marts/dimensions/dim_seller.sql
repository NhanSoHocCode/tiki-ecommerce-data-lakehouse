{{ config(unique_key='seller_id') }}

SELECT
    seller_id,
    ARBITRARY(seller_name) AS seller_name,
    ARBITRARY(is_from_official_store) AS is_from_official_store,
    CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMP(6)) AS updated_at
FROM {{ ref('stg_products') }}
WHERE seller_id IS NOT NULL
GROUP BY seller_id
