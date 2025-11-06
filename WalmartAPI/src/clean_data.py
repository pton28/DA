import pandas as pd
import os

# ====== XÁC ĐỊNH THƯ MỤC GỐC ======
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # thư mục cha của src
DATA_DIR = os.path.join(BASE_DIR, "data")

def clean_data():
    # ====== BƯỚC 1: TẠO THƯ MỤC ======
    clean_dir = os.path.join(DATA_DIR, "clean_data")
    os.makedirs(clean_dir, exist_ok=True)

    # ====== BƯỚC 2: ĐỌC DỮ LIỆU GỐC ======
    input_path = os.path.join(DATA_DIR, "save_data", "products.csv")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Không tìm thấy file: {input_path}. Hãy chắc chắn đã chạy save_data.py trước.")

    df = pd.read_csv(input_path, encoding="utf-8")

    # ====== BƯỚC 3: XỬ LÝ DỮ LIỆU ======
    if "fetch_time" in df.columns:
        df.sort_values(by="fetch_time", inplace=True)

    if "us_item_id" in df.columns:
        df = df.drop_duplicates(subset=["us_item_id"], keep="last")
    elif "product_id" in df.columns:
        df = df.drop_duplicates(subset=["product_id"], keep="last")

    df.reset_index(drop=True, inplace=True)

    # ====== BƯỚC 4: LƯU DỮ LIỆU CLEAN ======
    csv_path = os.path.join(clean_dir, "cleaned_products.csv")
    json_path = os.path.join(clean_dir, "cleaned_products.json")
    excel_path = os.path.join(clean_dir, "cleaned_products.xlsx")

    df.to_csv(csv_path, index=False, encoding="utf-8")
    df.to_json(json_path, orient="records", force_ascii=False, indent=2)
    df.to_excel(excel_path, index=False, engine="openpyxl")

    print(f"Đã lưu cleaned data vào:\n- {csv_path}\n- {json_path}\n- {excel_path}")
    print(f"Tổng số sản phẩm duy nhất: {len(df)}")

    # ====== BƯỚC 5: IN THỐNG KÊ ======
    print("\n===== THÔNG TIN SAU KHI CLEAN =====")
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
    clean_data()
