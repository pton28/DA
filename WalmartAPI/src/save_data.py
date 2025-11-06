import json
import pandas as pd
import os

# ======== XÁC ĐỊNH THƯ MỤC GỐC =========
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

def save_data():
    # ====== 1. Đường dẫn input/output ======
    raw_path = os.path.join(DATA_DIR, "raw_data", "raw_data.json")
    save_dir = os.path.join(DATA_DIR, "save_data")
    os.makedirs(save_dir, exist_ok=True)

    # ====== 2. Đọc dữ liệu raw JSON ======
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Không tìm thấy file: {raw_path}. Hãy chạy call_API.py trước.")
    
    with open(raw_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # ====== 3. Lưu dữ liệu ra nhiều định dạng ======
    json_path = os.path.join(save_dir, "products.json")
    csv_path = os.path.join(save_dir, "products.csv")
    excel_path = os.path.join(save_dir, "products.xlsx")

    # JSON (pretty format)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json.loads(df.to_json(orient="records", force_ascii=False)), f, ensure_ascii=False, indent=2)

    # CSV & Excel
    df.to_csv(csv_path, index=False, encoding="utf-8")
    df.to_excel(excel_path, index=False, engine="openpyxl")

    print("✅ Đã lưu dữ liệu ra các file:")
    print(f"- JSON : {json_path}")
    print(f"- CSV   : {csv_path}")
    print(f"- Excel : {excel_path}")
    print(f"📦 Tổng số dòng dữ liệu: {len(df)}")

if __name__ == "__main__":
    save_data()
