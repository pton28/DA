import pandas as pd
import os

# ======== XÁC ĐỊNH THƯ MỤC GỐC =========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # thư mục cha của src
DATA_DIR = os.path.join(BASE_DIR, "data")

def analyze_data():
    # ====== 1. Đường dẫn tới file cần phân tích ======
    csv_path = os.path.join(DATA_DIR, "save_data", "products.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Không tìm thấy file: {csv_path}\nHãy chắc chắn đã chạy save_data.py trước.")

    # ====== 2. Đọc dữ liệu ======
    df = pd.read_csv(csv_path, encoding="utf-8")

    # ====== 3. In thống kê ======
    print("===== THÔNG TIN TỔNG QUAN =====")
    print(f"Số dòng (rows): {df.shape[0]}")
    print(f"Số cột (columns): {df.shape[1]}")
    
    print("\nDanh sách các cột:")
    print(df.columns.tolist())

    print("\n===== KIỂU DỮ LIỆU =====")
    print(df.dtypes)

    print("\n===== THỐNG KÊ NHANH =====")
    print(df.describe(include="all").transpose())

    print("\n===== GIÁ TRỊ NULL TRONG MỖI CỘT =====")
    print(df.isnull().sum())

    print("\n===== SỐ LƯỢNG GIÁ TRỊ UNIQUE TRONG MỖI CỘT =====")
    print(df.nunique())

if __name__ == "__main__":
    analyze_data()
