
import os
import json
from datetime import datetime
from serpapi import GoogleSearch

# Đọc API key từ biến môi trường
API_KEY = os.getenv("API_KEY")

if not API_KEY:
    raise ValueError("⚠️ Không tìm thấy API_KEY! Hãy tạo file .env hoặc truyền API key qua docker run -e API_KEY=...")

# ======== CÁC LOẠI HÀNG =========
QUERIES = [
    "coffee", "groceries", "furniture", "toys", "clothes",
    "beauty products", "headphones", "refrigerator", "microwave",
    "washing machine", "pet supplies", "baby products",
    "cleaning supplies", "kitchen appliances", "bedding",
    "mattresses", "shoes", "sporting goods", "books",
    "stationery", "office supplies", "outdoor furniture", "garden tools"
]
N_PAGES = 5  # số trang cần crawl cho mỗi loại hàng

# ======== XÁC ĐỊNH ĐƯỜNG DẪN GỐC =========
# BASE_DIR: thư mục cha của src (project/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")


# ======== HÀM LẤY DỮ LIỆU =========
def fetch_walmart_data(query, n_pages=1):
    results_all = []
    for page in range(1, n_pages + 1):
        params = {
            "engine": "walmart",
            "query": query,
            "api_key": API_KEY,
            "page": page
        }

        search = GoogleSearch(params)
        results = search.get_dict()

        if "error" in results:
            print(f"Lỗi với query '{query}': {results['error']}")
            continue

        if "organic_results" in results:
            fetch_time = datetime.now().isoformat()
            for item in results["organic_results"]:
                item["query"] = query
                item["fetch_time"] = fetch_time
                results_all.append(item)
    return results_all


# ======== CHẠY CHƯƠNG TRÌNH =========
if __name__ == "__main__":
    # 1. Tạo thư mục lưu raw data
    raw_dir = os.path.join(DATA_DIR, "raw_data")
    os.makedirs(raw_dir, exist_ok=True)

    # 2. Gọi API và thu thập dữ liệu
    all_data = []
    for q in QUERIES:
        print(f"Đang lấy dữ liệu cho: {q} ...")
        products = fetch_walmart_data(q, N_PAGES)
        all_data.extend(products)

    # 3. Lưu file JSON vào data/raw_data/
    raw_path = os.path.join(raw_dir, "raw_data.json")
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Đã lưu dữ liệu thô vào: {raw_path}")
    print(f"📦 Tổng số sản phẩm thu được: {len(all_data)}")
