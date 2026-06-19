{{ config(unique_key='delivery_id') }}

SELECT
    FROM_BIG_ENDIAN_64(XXHASH64(TO_UTF8(
        COALESCE(order_route,           '') ||
        COALESCE(freeship_campaign,     '') ||
        COALESCE(fastest_delivery_time, '') ||
        CAST(is_tikinow_delivery AS VARCHAR) ||
        CAST(is_nextday_delivery AS VARCHAR) ||
        CAST(is_normal_delivery  AS VARCHAR)
    ))) AS delivery_id,
    is_tikinow_delivery,
    is_nextday_delivery,
    is_normal_delivery,
    order_route,
    freeship_campaign,
    fastest_delivery_time,
    CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMP(6)) AS updated_at
FROM {{ ref('stg_products') }}
GROUP BY
    is_tikinow_delivery,
    is_nextday_delivery,
    is_normal_delivery,
    order_route,
    freeship_campaign,
    fastest_delivery_time
