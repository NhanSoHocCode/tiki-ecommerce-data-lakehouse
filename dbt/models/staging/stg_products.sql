WITH source_products AS (
    SELECT
        NULLIF(TRIM(CAST(id AS VARCHAR)), '') AS id,
        NULLIF(TRIM(CAST(seller_id AS VARCHAR)), '') AS seller_id,
        NULLIF(TRIM(CAST(brand_id AS VARCHAR)), '') AS brand_id,

        NULLIF(TRIM(CAST(price AS VARCHAR)), '') AS price,
        NULLIF(TRIM(CAST(original_price AS VARCHAR)), '') AS original_price,
        NULLIF(TRIM(CAST(discount AS VARCHAR)), '') AS discount,
        NULLIF(TRIM(CAST(discount_rate AS VARCHAR)), '') AS discount_rate,
        NULLIF(TRIM(CAST(rating_average AS VARCHAR)), '') AS rating_average,
        NULLIF(TRIM(CAST(review_count AS VARCHAR)), '') AS review_count,
        NULLIF(TRIM(CAST(quantity_sold AS VARCHAR)), '') AS quantity_sold,

        NULLIF(TRIM(CAST(is_high_price_penalty AS VARCHAR)), '') AS is_high_price_penalty,
        NULLIF(TRIM(CAST(is_top_brand AS VARCHAR)), '') AS is_top_brand,
        NULLIF(TRIM(CAST(tiki_verified AS VARCHAR)), '') AS tiki_verified,
        NULLIF(TRIM(CAST(is_authentic AS VARCHAR)), '') AS is_authentic,
        NULLIF(TRIM(CAST(tiki_hero AS VARCHAR)), '') AS tiki_hero,
        NULLIF(TRIM(CAST(imported AS VARCHAR)), '') AS imported,
        NULLIF(TRIM(CAST(shippable AS VARCHAR)), '') AS shippable,
        NULLIF(TRIM(CAST(is_from_official_store AS VARCHAR)), '') AS is_from_official_store,
        NULLIF(TRIM(CAST(is_tikinow_delivery AS VARCHAR)), '') AS is_tikinow_delivery,
        NULLIF(TRIM(CAST(is_nextday_delivery AS VARCHAR)), '') AS is_nextday_delivery,
        CAST(NULL AS VARCHAR) AS is_normal_delivery,

        NULLIF(TRIM(CAST(sku AS VARCHAR)), '') AS sku,
        NULLIF(TRIM(CAST(name AS VARCHAR)), '') AS name,
        NULLIF(TRIM(CAST(url_key AS VARCHAR)), '') AS url_key,
        NULLIF(TRIM(CAST(url_path AS VARCHAR)), '') AS url_path,
        NULLIF(TRIM(CAST(thumbnail_url AS VARCHAR)), '') AS thumbnail_url,
        NULLIF(TRIM(CAST(origin AS VARCHAR)), '') AS origin,
        NULLIF(TRIM(CAST(seller_name AS VARCHAR)), '') AS seller_name,
        NULLIF(TRIM(CAST(brand_name AS VARCHAR)), '') AS brand_name,
        NULLIF(TRIM(CAST(primary_category_path AS VARCHAR)), '') AS primary_category_path,
        NULLIF(TRIM(CAST(primary_category_name AS VARCHAR)), '') AS primary_category_name,
        NULLIF(TRIM(CAST(order_route AS VARCHAR)), '') AS order_route,
        NULLIF(TRIM(CAST(freeship_campaign AS VARCHAR)), '') AS freeship_campaign,
        NULLIF(TRIM(CAST(fastest_delivery_time AS VARCHAR)), '') AS fastest_delivery_time,
        NULLIF(TRIM(CAST(extracted_at AS VARCHAR)), '') AS extracted_at,
        NULLIF(TRIM(CAST(badges_new AS VARCHAR)), '') AS badges_new,
        NULLIF(TRIM(CAST(visible_impression_info AS VARCHAR)), '') AS visible_impression_info
    FROM {{ source('raw_tiki', 'products') }}
    WHERE "$path" LIKE '%' || FORMAT_DATETIME(CURRENT_TIMESTAMP, 'yyyyMMdd') || '%'
),

normalized_products AS (
    SELECT
        id,
        seller_id,
        brand_id,
        price,
        original_price,
        discount,
        discount_rate,
        rating_average,
        review_count,
        quantity_sold,
        is_high_price_penalty,
        is_top_brand,
        tiki_verified,
        is_authentic,
        tiki_hero,
        imported,
        shippable,
        is_from_official_store,
        is_tikinow_delivery,
        is_nextday_delivery,
        is_normal_delivery,
        sku,
        name,
        url_key,
        url_path,
        thumbnail_url,
        seller_name,
        primary_category_path,
        primary_category_name,
        order_route,
        freeship_campaign,
        fastest_delivery_time,
        extracted_at,
        COALESCE(
            brand_name,
            NULLIF(REGEXP_EXTRACT(visible_impression_info, '''brand_name'': ''([^'']+)''', 1), ''),
            NULLIF(REGEXP_EXTRACT(badges_new, '''code'': ''brand_name''.*?''text'': ''([^'']+)''', 1), '')
        ) AS brand_name,
        COALESCE(
            origin,
            NULLIF(REGEXP_EXTRACT(visible_impression_info, '''origin'': ''([^'']+)''', 1), '')
        ) AS origin
    FROM source_products
),

typed_products AS (
    SELECT
        TRY_CAST(id AS BIGINT) AS product_id,
        TRY_CAST(seller_id AS INTEGER) AS seller_id,
        COALESCE(
            TRY_CAST(TRY_CAST(brand_id AS DOUBLE) AS BIGINT),
            CASE
                WHEN brand_name IS NOT NULL THEN
                    -1 * (
                        MOD(
                            ABS(FROM_BIG_ENDIAN_64(XXHASH64(TO_UTF8(LOWER(brand_name))))),
                            2147483646
                        ) + 2
                    )
                ELSE -1
            END
        ) AS brand_id,

        COALESCE(TRY_CAST(price AS BIGINT), 0) AS price,
        COALESCE(TRY_CAST(original_price AS BIGINT), 0) AS original_price,
        COALESCE(TRY_CAST(discount AS BIGINT), 0) AS discount,
        COALESCE(TRY_CAST(discount_rate AS DOUBLE), 0.0) AS discount_rate,
        COALESCE(TRY_CAST(rating_average AS DOUBLE), 0.0) AS rating_average,
        COALESCE(TRY_CAST(review_count AS INTEGER), 0) AS review_count,
        COALESCE(
            TRY_CAST(REGEXP_EXTRACT(quantity_sold, '''value'':\s*(\d+)', 1) AS INTEGER),
            TRY_CAST(quantity_sold AS INTEGER),
            0
        ) AS quantity_sold,

        CASE
            WHEN LOWER(is_high_price_penalty) IN ('true', '1') THEN TRUE
            ELSE FALSE
        END AS is_high_price_penalty,
        CASE
            WHEN LOWER(is_top_brand) IN ('true', '1') THEN TRUE
            ELSE FALSE
        END AS is_top_brand,
        CASE
            WHEN LOWER(tiki_verified) IN ('true', '1') THEN TRUE
            ELSE FALSE
        END AS tiki_verified,
        CASE
            WHEN LOWER(is_authentic) IN ('true', '1') THEN TRUE
            ELSE FALSE
        END AS is_authentic,
        CASE
            WHEN LOWER(tiki_hero) IN ('true', '1') THEN TRUE
            ELSE FALSE
        END AS tiki_hero,

        COALESCE(sku, 'unknown') AS sku,
        COALESCE(name, 'unknown') AS name,
        COALESCE(url_key, 'unknown') AS url_key,
        COALESCE(url_path, 'unknown') AS url_path,
        COALESCE(thumbnail_url, 'unknown') AS thumbnail_url,
        CASE
            WHEN LOWER(imported) IN ('true', '1') THEN TRUE
            ELSE FALSE
        END AS imported,
        COALESCE(origin, 'unknown') AS origin,
        CASE
            WHEN LOWER(shippable) IN ('true', '1') THEN TRUE
            ELSE FALSE
        END AS shippable,

        COALESCE(seller_name, 'unknown') AS seller_name,
        CASE
            WHEN LOWER(is_from_official_store) IN ('true', '1') THEN TRUE
            ELSE FALSE
        END AS is_from_official_store,

        COALESCE(brand_name, 'unknown') AS brand_name,

        COALESCE(primary_category_path, 'unknown') AS primary_category_path,
        COALESCE(primary_category_name, 'unknown') AS primary_category_name,

        CASE
            WHEN LOWER(is_tikinow_delivery) IN ('true', '1') THEN TRUE
            ELSE FALSE
        END AS is_tikinow_delivery,
        CASE
            WHEN LOWER(is_nextday_delivery) IN ('true', '1') THEN TRUE
            ELSE FALSE
        END AS is_nextday_delivery,
        CASE
            WHEN LOWER(is_normal_delivery) IN ('true', '1') THEN TRUE
            ELSE FALSE
        END AS is_normal_delivery,
        COALESCE(order_route, 'unknown') AS order_route,
        COALESCE(freeship_campaign, 'none') AS freeship_campaign,
        COALESCE(fastest_delivery_time, 'unknown') AS fastest_delivery_time,

        COALESCE(
            TRY_CAST(extracted_at AS TIMESTAMP),
            TRY(DATE_PARSE(extracted_at, '%Y%m%d'))
        ) AS extracted_at
    FROM normalized_products
)

SELECT *
FROM typed_products
WHERE product_id IS NOT NULL
  AND seller_id IS NOT NULL
  AND extracted_at IS NOT NULL
