{{ config(unique_key='product_id') }}

SELECT
    product_id,
    ARBITRARY(sku) AS sku,
    ARBITRARY(name) AS name,
    ARBITRARY(url_key) AS url_key,
    ARBITRARY(url_path) AS url_path,
    ARBITRARY(thumbnail_url) AS thumbnail_url,
    ARBITRARY(imported) AS imported,
    ARBITRARY(origin) AS origin,
    ARBITRARY(shippable) AS shippable,
    CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMP(6)) AS updated_at
FROM {{ ref('stg_products') }}
WHERE product_id IS NOT NULL
GROUP BY product_id
