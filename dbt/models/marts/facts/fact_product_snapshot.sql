{{ config(unique_key='snapshot_id') }}

WITH current_snapshot AS (
    SELECT
        FROM_BIG_ENDIAN_64(XXHASH64(TO_UTF8(
            CAST(stg.product_id AS VARCHAR) ||
            CAST(stg.extracted_at AS VARCHAR)
        ))) AS snapshot_id,

        stg.product_id,
        stg.seller_id,
        stg.brand_id,
        FROM_BIG_ENDIAN_64(XXHASH64(TO_UTF8(stg.primary_category_path))) AS category_id,
        dt.date_id,
        FROM_BIG_ENDIAN_64(XXHASH64(TO_UTF8(
            COALESCE(stg.order_route,           '') ||
            COALESCE(stg.freeship_campaign,     '') ||
            COALESCE(stg.fastest_delivery_time, '') ||
            CAST(stg.is_tikinow_delivery AS VARCHAR) ||
            CAST(stg.is_nextday_delivery AS VARCHAR) ||
            CAST(stg.is_normal_delivery  AS VARCHAR)
        ))) AS delivery_id,

        stg.price,
        stg.original_price,
        stg.discount,
        stg.discount_rate,
        stg.rating_average,
        stg.review_count,
        stg.quantity_sold,

        stg.is_high_price_penalty,
        stg.is_top_brand,
        stg.tiki_verified,
        stg.is_authentic,
        stg.tiki_hero,

        stg.extracted_at,
        CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMP(6)) AS dbt_run_at

    FROM {{ ref('stg_products') }} stg
    LEFT JOIN {{ ref('dim_date') }} dt
        ON CAST(stg.extracted_at AS DATE) = dt.full_date

    WHERE stg.product_id IS NOT NULL
      AND stg.price > 0
),

with_previous_snapshot AS (
    SELECT
        cur.*,
        {% if is_incremental() %}
        prev.quantity_sold AS previous_quantity_sold,
        ROW_NUMBER() OVER (
            PARTITION BY cur.snapshot_id
            ORDER BY prev.date_id DESC NULLS LAST
        ) AS previous_rank
        {% else %}
        LAG(cur.quantity_sold) OVER (
            PARTITION BY cur.product_id
            ORDER BY cur.date_id ASC
        ) AS previous_quantity_sold,
        1 AS previous_rank
        {% endif %}
    FROM current_snapshot cur
    
    {% if is_incremental() %}
    LEFT JOIN {{ this }} prev
        ON cur.product_id = prev.product_id
       AND prev.date_id < cur.date_id
       AND CAST(prev.extracted_at AS DATE) >= CURRENT_DATE - INTERVAL '30' DAY
    {% endif %}
)

SELECT
    snapshot_id,
    product_id,
    seller_id,
    brand_id,
    category_id,
    date_id,
    delivery_id,
    price,
    original_price,
    discount,
    discount_rate,
    rating_average,
    review_count,
    quantity_sold,
    CASE
        WHEN previous_quantity_sold IS NULL THEN 0
        WHEN quantity_sold >= previous_quantity_sold
            THEN quantity_sold - previous_quantity_sold
        ELSE 0
    END AS estimated_sold_increment,
    is_high_price_penalty,
    is_top_brand,
    tiki_verified,
    is_authentic,
    tiki_hero,
    extracted_at,
    dbt_run_at
FROM with_previous_snapshot
WHERE previous_rank = 1
