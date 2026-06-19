# Thong Tin Bang Dimension Va Fact

File nay mo ta cac bang mart trong mo hinh Star Schema cua project Tiki Lakehouse. Cac bang duoc tao boi dbt va query qua Trino/Iceberg.

## Tong Quan

| Nhom | Schema | Bang | Vai tro |
|---|---|---|---|
| Fact | `lakehouse.facts` | `fact_product_snapshot` | Luu snapshot san pham theo ngay/thoi diem crawl, gom metric ban hang va gia |
| Dimension | `lakehouse.dimensions` | `dim_product` | Thong tin san pham |
| Dimension | `lakehouse.dimensions` | `dim_category` | Thong tin danh muc san pham |
| Dimension | `lakehouse.dimensions` | `dim_brand` | Thong tin thuong hieu |
| Dimension | `lakehouse.dimensions` | `dim_seller` | Thong tin nha ban hang |
| Dimension | `lakehouse.dimensions` | `dim_date` | Chieu thoi gian |
| Dimension | `lakehouse.dimensions` | `dim_delivery` | Thong tin giao hang |

## Grain Va Quan He

### Grain cua fact

`fact_product_snapshot` co grain:

```text
1 dong = 1 san pham tai 1 thoi diem/ngay crawl
```

Khoa chinh logic:

```text
snapshot_id = HASH(product_id || extracted_at)
```

### Quan he star schema

```text
fact_product_snapshot.product_id  -> dim_product.product_id
fact_product_snapshot.category_id -> dim_category.category_id
fact_product_snapshot.brand_id    -> dim_brand.brand_id
fact_product_snapshot.seller_id   -> dim_seller.seller_id
fact_product_snapshot.date_id     -> dim_date.date_id
fact_product_snapshot.delivery_id -> dim_delivery.delivery_id
```

## Fact Table

### `lakehouse.facts.fact_product_snapshot`

Mo ta: bang fact luu snapshot san pham, gia ban tai thoi diem crawl va cac metric ban hang.

| Cot | Kieu du lieu | Vai tro | Mo ta |
|---|---:|---|---|
| `snapshot_id` | bigint | PK | Surrogate key cua snapshot, hash tu `product_id` va `extracted_at` |
| `product_id` | bigint | FK | Khoa lien ket toi `dim_product` |
| `seller_id` | integer | FK | Khoa lien ket toi `dim_seller` |
| `brand_id` | bigint | FK | Khoa lien ket toi `dim_brand` |
| `category_id` | bigint | FK | Khoa lien ket toi `dim_category` |
| `date_id` | integer | FK | Khoa lien ket toi `dim_date` |
| `delivery_id` | bigint | FK | Khoa lien ket toi `dim_delivery` |
| `price` | bigint | Metric/attribute | Gia ban tai snapshot |
| `original_price` | bigint | Metric/attribute | Gia goc tai snapshot |
| `discount` | bigint | Metric/attribute | Gia tri giam gia |
| `discount_rate` | double | Metric/attribute | Ty le giam gia |
| `rating_average` | double | Metric/attribute | Diem danh gia trung binh |
| `review_count` | integer | Metric/attribute | So luong review |
| `quantity_sold` | integer | Cumulative metric | So luong ban luy ke tai thoi diem crawl |
| `estimated_sold_increment` | integer | Daily delta metric | So luong ban tang them so voi snapshot truoc |
| `is_high_price_penalty` | boolean | Flag | Co bi penalty gia cao hay khong |
| `is_top_brand` | boolean | Flag | Co thuoc top brand hay khong |
| `tiki_verified` | boolean | Flag | San pham/nguon ban co duoc Tiki verified hay khong |
| `is_authentic` | boolean | Flag | Co duoc danh dau chinh hang hay khong |
| `tiki_hero` | boolean | Flag | Co la Tiki hero hay khong |
| `extracted_at` | timestamp | Time | Thoi diem crawl du lieu |
| `dbt_run_at` | timestamp | Audit | Thoi diem dbt tao/cap nhat dong du lieu |

### Luu y ve metric

`quantity_sold` la so ban luy ke, khong nen dung truc tiep lam so ban trong ngay.

Metric nen dung cho phan tich daily sales:

```sql
SUM(estimated_sold_increment)
```

Doanh thu uoc tinh theo ngay:

```sql
SUM(estimated_sold_increment * price)
```

Gia ban trung binh co trong so theo so luong ban:

```sql
SUM(price * estimated_sold_increment)
/
NULLIF(SUM(estimated_sold_increment), 0)
```

`estimated_sold_increment` duoc tinh trong dbt bang cach so sanh `quantity_sold` hien tai voi snapshot truoc cua cung `product_id`. Neu khong co snapshot truoc, hoac `quantity_sold` bi giam, gia tri se la `0`.

## Dimension Tables

### `lakehouse.dimensions.dim_product`

Mo ta: chieu san pham, cap nhat theo SCD Type 1.

| Cot | Kieu du lieu | Vai tro | Mo ta |
|---|---:|---|---|
| `product_id` | bigint | PK | Natural key san pham tu Tiki API |
| `sku` | string | Attribute | SKU san pham |
| `name` | string | Attribute | Ten san pham |
| `url_key` | string | Attribute | URL key cua san pham |
| `url_path` | string | Attribute | Duong dan san pham |
| `thumbnail_url` | string | Attribute | Anh thumbnail |
| `imported` | boolean | Attribute | San pham nhap khau hay khong |
| `origin` | string | Attribute | Xuat xu |
| `shippable` | boolean | Attribute | Co the giao hang hay khong |
| `updated_at` | timestamp | Audit | Thoi diem dbt cap nhat dimension |

### `lakehouse.dimensions.dim_category`

Mo ta: chieu danh muc, flatten toi 4 cap tu `primary_category_path`.

| Cot | Kieu du lieu | Vai tro | Mo ta |
|---|---:|---|---|
| `category_id` | bigint | PK | Surrogate key = hash tu `primary_category_path` |
| `primary_category_name` | string | Attribute | Ten danh muc chinh |
| `primary_category_path` | string | Attribute | Path danh muc day du |
| `category_level_1` | string | Attribute | Danh muc cap 1 |
| `category_level_2` | string | Attribute | Danh muc cap 2 |
| `category_level_3` | string | Attribute | Danh muc cap 3 |
| `category_level_4` | string | Attribute | Danh muc cap 4 |
| `updated_at` | timestamp | Audit | Thoi diem dbt cap nhat dimension |

### `lakehouse.dimensions.dim_brand`

Mo ta: chieu thuong hieu.

| Cot | Kieu du lieu | Vai tro | Mo ta |
|---|---:|---|---|
| `brand_id` | bigint | PK | Ma thuong hieu |
| `brand_name` | string | Attribute | Ten thuong hieu |
| `updated_at` | timestamp | Audit | Thoi diem dbt cap nhat dimension |

### `lakehouse.dimensions.dim_seller`

Mo ta: chieu nha ban hang/shop.

| Cot | Kieu du lieu | Vai tro | Mo ta |
|---|---:|---|---|
| `seller_id` | integer | PK | Ma nha ban hang |
| `seller_name` | string | Attribute | Ten nha ban hang/shop |
| `is_from_official_store` | boolean | Attribute | Co phai official store hay khong |
| `updated_at` | timestamp | Audit | Thoi diem dbt cap nhat dimension |

### `lakehouse.dimensions.dim_date`

Mo ta: chieu thoi gian tu `2020-01-01` den `2030-12-31`.

| Cot | Kieu du lieu | Vai tro | Mo ta |
|---|---:|---|---|
| `date_id` | integer | PK | Khoa ngay dang `YYYYMMDD` |
| `full_date` | date | Attribute | Ngay day du |
| `year` | integer | Attribute | Nam |
| `month` | integer | Attribute | Thang |
| `day` | integer | Attribute | Ngay trong thang |
| `quarter` | integer | Attribute | Quy |
| `day_of_week` | string | Attribute | Ten thu trong tuan |
| `month_name` | string | Attribute | Ten thang |
| `is_weekend` | boolean | Attribute | Co phai cuoi tuan hay khong |
| `updated_at` | timestamp | Audit | Thoi diem dbt cap nhat dimension |

### `lakehouse.dimensions.dim_delivery`

Mo ta: chieu giao hang, key duoc tao tu to hop cac thuoc tinh giao hang.

| Cot | Kieu du lieu | Vai tro | Mo ta |
|---|---:|---|---|
| `delivery_id` | bigint | PK | Surrogate key = hash tu to hop thuoc tinh giao hang |
| `is_tikinow_delivery` | boolean | Attribute | Co giao TikiNOW hay khong |
| `is_nextday_delivery` | boolean | Attribute | Co giao ngay tiep theo hay khong |
| `is_normal_delivery` | boolean | Attribute | Co giao thuong hay khong |
| `order_route` | string | Attribute | Lo trinh/order route giao hang |
| `freeship_campaign` | string | Attribute | Chien dich freeship |
| `fastest_delivery_time` | string | Attribute | Thoi gian giao nhanh nhat |
| `updated_at` | timestamp | Audit | Thoi diem dbt cap nhat dimension |

## Query Mau

### Doanh thu va so ban theo ngay

```sql
SELECT
    d.full_date,
    SUM(f.estimated_sold_increment) AS daily_units_sold,
    SUM(f.estimated_sold_increment * f.price) AS estimated_daily_revenue
FROM lakehouse.facts.fact_product_snapshot f
LEFT JOIN lakehouse.dimensions.dim_date d
    ON f.date_id = d.date_id
GROUP BY d.full_date
ORDER BY d.full_date;
```

### Top san pham ban tang them nhieu nhat

```sql
SELECT
    d.full_date,
    p.product_id,
    p.name AS product_name,
    SUM(f.estimated_sold_increment) AS daily_units_sold
FROM lakehouse.facts.fact_product_snapshot f
LEFT JOIN lakehouse.dimensions.dim_date d
    ON f.date_id = d.date_id
LEFT JOIN lakehouse.dimensions.dim_product p
    ON f.product_id = p.product_id
GROUP BY
    d.full_date,
    p.product_id,
    p.name
ORDER BY daily_units_sold DESC
LIMIT 20;
```

### Phan tich theo category/brand/seller

```sql
SELECT
    d.full_date,
    c.category_level_1,
    b.brand_name,
    s.seller_name,
    SUM(f.estimated_sold_increment) AS daily_units_sold,
    SUM(f.estimated_sold_increment * f.price) AS estimated_daily_revenue
FROM lakehouse.facts.fact_product_snapshot f
LEFT JOIN lakehouse.dimensions.dim_date d
    ON f.date_id = d.date_id
LEFT JOIN lakehouse.dimensions.dim_category c
    ON f.category_id = c.category_id
LEFT JOIN lakehouse.dimensions.dim_brand b
    ON f.brand_id = b.brand_id
LEFT JOIN lakehouse.dimensions.dim_seller s
    ON f.seller_id = s.seller_id
GROUP BY
    d.full_date,
    c.category_level_1,
    b.brand_name,
    s.seller_name;
```

## Luu Y Su Dung Cho BI

- Dung `estimated_sold_increment` cho cac chart ban theo ngay, tang truong, top product/category/brand/seller.
- Dung `quantity_sold` de xem trang thai luy ke tai snapshot, khong dung lam daily sales.
- Khi tinh revenue, nen dung `estimated_sold_increment * price` vi `price` la gia tai snapshot.
- Khi tinh gia trung binh de phan tich price dynamics, nen dung weighted average theo `estimated_sold_increment` neu muc tieu la gia thuc te gan voi so luong ban.
- Cac dimension hien duoc materialize incremental voi chien luoc merge, chu yeu la SCD Type 1.
