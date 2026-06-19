-- One-time setup: expose crawler parquet files as raw Hive staging source.
-- dbt reads this source and writes the final models into Iceberg.

CREATE SCHEMA IF NOT EXISTS hive.raw
WITH (location = 's3://raw-data/tiki_products/');

DROP TABLE IF EXISTS hive.raw.products;

CREATE TABLE hive.raw.products (
    id BIGINT,
    sku VARCHAR,
    name VARCHAR,
    url_key VARCHAR,
    url_path VARCHAR,
    availability BIGINT,
    seller_id BIGINT,
    seller_name VARCHAR,
    price BIGINT,
    original_price BIGINT,
    badges_new VARCHAR,
    badges_v3 VARCHAR,
    discount BIGINT,
    discount_rate BIGINT,
    rating_average DOUBLE,
    review_count BIGINT,
    category_ids VARCHAR,
    primary_category_path VARCHAR,
    primary_category_name VARCHAR,
    thumbnail_url VARCHAR,
    thumbnail_width BIGINT,
    thumbnail_height BIGINT,
    productset_id BIGINT,
    seller_product_id BIGINT,
    seller_product_sku VARCHAR,
    master_product_sku VARCHAR,
    video_url VARCHAR,
    shippable BOOLEAN,
    isGiftAvailable BOOLEAN,
    fastest_delivery_time VARCHAR,
    order_route VARCHAR,
    is_tikinow_delivery BOOLEAN,
    is_nextday_delivery BOOLEAN,
    is_from_official_store BOOLEAN,
    is_authentic BIGINT,
    tiki_verified BIGINT,
    tiki_hero BIGINT,
    freeship_campaign VARCHAR,
    impression_info VARCHAR,
    visible_impression_info VARCHAR,
    layout_type VARCHAR,
    is_high_price_penalty BOOLEAN,
    is_top_brand BOOLEAN,
    quantity_sold VARCHAR,
    brand_id DOUBLE,
    brand_name VARCHAR,
    origin VARCHAR,
    imported VARCHAR,
    extracted_at VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 's3://raw-data/tiki_products/'
);
