import requests
import pandas as pd
import os
from io import BytesIO
import boto3
import dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import time

dotenv.load_dotenv()

# variable default
DEFAULT_CATEGORY_ID = 8322
MAX_WORKERS = 5
# .env
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

def fetch_all_tiki_products(root_category_id=DEFAULT_CATEGORY_ID, max_workers=MAX_WORKERS):
    """
    Đệ quy đào sâu lấy TOÀN BỘ danh mục cấp cuối cùng (Leaf Categories),
    sau đó kích hoạt đa luồng để vét sạch sản phẩm toàn sàn không bị sót.
    """
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "vi-VN,vi;q=0.9"
    }
    
    # BƯỚC 1: HÀM ĐỆ QUY ĐÀO SÂU XUỐNG TẦNG DANH MỤC CUỐI CÙNG (LEAF CATEGORIES)
    leaf_categories = []

    def get_leaf_categories(category_id):
        """Hàm đệ quy quét xuyên qua các tầng danh mục lồng nhau"""
        time.sleep(random.uniform(0.1, 0.3)) # Delay nhẹ để tránh dồn dập API danh mục
        try:
            cate_url = f"https://tiki.vn/api/v2/categories?parent_id={category_id}"
            res = requests.get(cate_url, headers=header, timeout=10)
            if res.status_code == 200:
                sub_cates = res.json().get('data', [])
                if sub_cates:
                    # Nếu có danh mục con cấp dưới, tiếp tục đệ quy đào sâu xuống tiếp
                    for sub in sub_cates:
                        sub_id = sub.get('id')
                        if sub_id:
                            get_leaf_categories(sub_id)
                else:
                    # Nếu không còn danh mục con nào nữa, đây chính là danh mục cấp cuối (Leaf)
                    leaf_categories.append(category_id)
                    print(f"    -> Xác nhận danh mục cấp cuối: {category_id}")
            else:
                leaf_categories.append(category_id)
        except:
            leaf_categories.append(category_id)

    print(f"[1] Bắt đầu quét đệ quy toàn bộ cây danh mục của gốc {root_category_id}...")
    start_tree_time = time.time()
    get_leaf_categories(root_category_id)
    
    # Lọc trùng danh sách danh mục đề phòng API trả trùng
    leaf_categories = list(set(leaf_categories))
    print(f"--> Hoàn thành quét cây trong {time.time() - start_tree_time:.2f} giây.")
    print(f"--> Tổng số danh mục cấp cuối cùng tìm thấy: {len(leaf_categories)} nhóm.\n")

    # BƯỚC 2: LOGIC PHÂN TRANG VÔ HẠN CHO MỖI DANH MỤC CẤP CUỐI
    def scrape_category(category_id):
        """Cào sạch toàn bộ các trang sản phẩm của danh mục nhỏ"""
        cate_products = []
        page = 1
        while True:
            tiki_url = f'https://tiki.vn/api/v2/products?limit=40&page={page}&category={category_id}'
            time.sleep(random.uniform(0.3, 0.8)) # Delay thông minh để tối ưu tốc độ đa luồng
            
            try:
                response = requests.get(url=tiki_url, headers=header, timeout=10)
                if response.status_code == 200:
                    data = response.json().get('data', [])
                    if not data: # Hết sản phẩm ở trang này -> Thoát vòng lặp
                        break
                    cate_products.extend(data)
                    page += 1
                    
                    # Giới hạn chặn cứng của Tiki cho 1 endpoint danh mục là 50 trang
                    if page > 50:
                        break
                else:
                    break
            except:
                break
        return cate_products

    # BƯỚC 3: KÍCH HOẠT ĐA LUỒNG DUYỆT SONG SONG QUA CÁC DANH MỤC CẤP CUỐI
    all_scraped_data = []
    print(f"[2] Kích hoạt {max_workers} luồng xử lý song song để gom dữ liệu...")
    start_scrape_time = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Giao việc cho các worker: Mỗi luồng xử lý trọn gói cào sản phẩm của một danh mục cấp cuối
        future_to_cate = {executor.submit(scrape_category, cid): cid for cid in leaf_categories}
        
        for idx, future in enumerate(as_completed(future_to_cate), 1):
            cid = future_to_cate[future]
            try:
                cate_data = future.result()
                if cate_data:
                    all_scraped_data.extend(cate_data)
                    print(f"    [{idx}/{len(leaf_categories)}] -> Danh mục {cid}: Thu về {len(cate_data)} sản phẩm.")
                else:
                    print(f"    [{idx}/{len(leaf_categories)}] -> Danh mục {cid}: Trống hoặc không hoạt động.")
            except Exception as e:
                print(f"[-] Lỗi luồng tại danh mục {cid}: {e}")

    # BƯỚC 4: LÀM SẠCH DỮ LIỆU, LỌC TRÙNG SẢN PHẨM VÀ XUẤT CSV
    print(f"\n[3] Tiến hành hợp nhất dữ liệu và dọn dẹp trùng lặp...")
    if all_scraped_data:
        df = pd.DataFrame(all_scraped_data)
        
        # Loại bỏ các sản phẩm bị trùng lặp do nằm ở giao thoa nhiều danh mục
        total_before = len(df)
        df.drop_duplicates(subset=['id'], keep='first', inplace=True)
        total_after = len(df)
        
        # Gắn thẻ mốc thời gian snapshot
        now_str = time.strftime("%Y%m%d")
        df['extracted_at'] = now_str
        
        print(f"\n[SUCCESS] TOÀN BỘ TIẾN TRÌNH HOÀN TẤT MỸ MÃN!")
        print(f"--> Tổng số bản ghi thu thập được: {total_before}")
        print(f"--> Số sản phẩm thực tế sau khi làm sạch (Unique): {total_after}")
        print(f"--> Thời gian cào sản phẩm: {(time.time() - start_scrape_time)/60:.2f} phút.")

        return df
    else:
        print("\n[-] Quy trình thất bại: Hệ thống không lấy được bất kỳ dữ liệu nào.")
        return None

def save_to_minio(data):
    if data is None or data.empty:
        print("No data to save")
        return
    df = pd.DataFrame(data)
    time_now = time.strftime("%Y%m%d")
    df['extracted_at'] = time_now
    
    # Convert tất cả cột chứa dict/list (kể cả dict rỗng {}) sang string
    # Lý do: PyArrow không thể ghi struct rỗng vào Parquet
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(
                lambda x: str(x) if isinstance(x, (dict, list)) else x
            )
    
    preview_dir = "preview_data"
    os.makedirs(preview_dir, exist_ok=True)
    preview_path = os.path.join(preview_dir, f'products_{time.strftime("%Y%m%d")}.csv')
    # df.to_csv(preview_path, index=False)
    # print(f"Saved data to {preview_path}")

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    # Connect to MinIO
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD
    )

    file_name = f"tiki_products/products_{time_now}.parquet"
    print(f"Uploading {len(df)} products to raw-data/{file_name}")
    
    # Create bucket raw-data, if exits then pass
    try:
        s3_client.create_bucket(
            Bucket="raw-data"
        )
        print("Bucket 'raw-data' created")
    except Exception as e:
        print("Bucket 'raw-data' already exists")
        pass
    # Create bucket lakehouse, if exits then pass
    try:
        s3_client.create_bucket(
            Bucket="lakehouse"
        )
        print("Bucket 'lakehouse' created")
    except Exception as e:
        print("Bucket 'lakehouse' already exists")
        pass

    # Upload the parquet file to s3
    s3_client.put_object(
        Bucket="raw-data",
        Key=file_name,
        Body=parquet_buffer.getvalue()
    )
    print(f"Saved data to MinIO: {file_name}")

if __name__ == "__main__":
    data = fetch_all_tiki_products()
    if data is not None:
        save_to_minio(data)
    else: 
        print("Data is None")
    
    