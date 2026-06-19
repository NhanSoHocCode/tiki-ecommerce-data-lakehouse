{{ config(unique_key='category_id') }}

SELECT
    FROM_BIG_ENDIAN_64(XXHASH64(TO_UTF8(primary_category_path))) AS category_id,
    ARBITRARY(primary_category_name) AS primary_category_name,
    primary_category_path,
    ARBITRARY(SPLIT_PART(primary_category_path, '/', 1)) AS category_level_1,
    ARBITRARY(SPLIT_PART(primary_category_path, '/', 2)) AS category_level_2,
    ARBITRARY(SPLIT_PART(primary_category_path, '/', 3)) AS category_level_3,
    ARBITRARY(SPLIT_PART(primary_category_path, '/', 4)) AS category_level_4,
    CAST(CURRENT_TIMESTAMP(6) AS TIMESTAMP(6)) AS updated_at
FROM {{ ref('stg_products') }}
WHERE primary_category_path IS NOT NULL
GROUP BY primary_category_path
