import pandas as pd
import matplotlib.pyplot as plt
import ast
import os

def extract_price(x):
    """Hàm lấy giá từ cột primary_offer"""
    try:
        d = ast.literal_eval(x) if isinstance(x, str) else {}
        return d.get("offer_price") or d.get("min_price")
    except Exception:
        return None


def eda_api():
    # ====== XÁC ĐỊNH ĐƯỜNG DẪN GỐC ======
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Thư mục cha của src
    DATA_DIR = os.path.join(BASE_DIR, "data")
    eda_dir = os.path.join(DATA_DIR, "eda_pic")
    os.makedirs(eda_dir, exist_ok=True)

    # ====== BƯỚC 2: ĐỌC DỮ LIỆU ĐÃ CLEAN ======
    input_path = os.path.join(DATA_DIR, "clean_data", "cleaned_products.csv")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Không tìm thấy file {input_path}. Hãy chạy clean_data.py trước.")
    
    df = pd.read_csv(input_path, encoding="utf-8")
    print("===== TỔNG QUAN DỮ LIỆU CLEAN =====")
    print(df.info())
    print(df.head())

    # ====== BƯỚC 3: PHÂN TÍCH RATING ======
    print("\n--- Thống kê rating ---")
    if "rating" in df.columns:
        print(df["rating"].describe())
        plt.hist(df["rating"].dropna(), bins=20, edgecolor="black")
        plt.title("Phân bố Rating của sản phẩm")
        plt.xlabel("Rating")
        plt.ylabel("Số lượng sản phẩm")
        plt.tight_layout()
        plt.savefig(os.path.join(eda_dir, "eda_rating_hist.png"))
        plt.close()

    # ====== BƯỚC 4: TOP SẢN PHẨM CÓ NHIỀU REVIEWS NHẤT ======
    if {"title", "reviews", "rating"}.issubset(df.columns):
        top_reviews = df[["title", "reviews", "rating"]].sort_values(by="reviews", ascending=False).head(10)
        print("\n--- Top 10 sản phẩm có nhiều reviews nhất ---")
        print(top_reviews)
        top_reviews.to_csv(os.path.join(eda_dir, "eda_top_reviews.csv"), index=False, encoding="utf-8")

    # ====== BƯỚC 5: PHÂN BỐ SẢN PHẨM THEO LOẠI HÀNG ======
    if "query" in df.columns:
        query_counts = df["query"].value_counts()
        print("\n--- Số lượng sản phẩm theo loại hàng ---")
        print(query_counts)
        query_counts.plot(kind="bar", figsize=(12, 6))
        plt.title("Số lượng sản phẩm theo loại hàng (query)")
        plt.xlabel("Loại hàng")
        plt.ylabel("Số sản phẩm")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(os.path.join(eda_dir, "eda_query_counts.png"))
        plt.close()

    # ====== BƯỚC 6: PHÂN TÍCH SELLER ======
    if "seller_name" in df.columns:
        seller_counts = df["seller_name"].value_counts().head(10)
        print("\n--- Top 10 seller có nhiều sản phẩm nhất ---")
        print(seller_counts)
        seller_counts.to_csv(os.path.join(eda_dir, "eda_top_sellers.csv"), encoding="utf-8")
        seller_counts.plot(kind="bar", figsize=(10, 5))
        plt.title("Top 10 seller có nhiều sản phẩm nhất")
        plt.xlabel("Seller")
        plt.ylabel("Số sản phẩm")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(os.path.join(eda_dir, "eda_seller_counts.png"))
        plt.close()

    # ====== BƯỚC 7: PHÂN TÍCH GIÁ ======
    if "primary_offer" in df.columns:
        df["price"] = df["primary_offer"].apply(extract_price)
        print("\n--- Thống kê giá sản phẩm ---")
        print(df["price"].describe())

        avg_price_query = df.groupby("query")["price"].mean().sort_values(ascending=False)
        print("\n--- Giá trung bình theo loại hàng ---")
        print(avg_price_query)
        avg_price_query.to_csv(os.path.join(eda_dir, "eda_avg_price_query.csv"), encoding="utf-8")
        avg_price_query.plot(kind="bar", figsize=(12, 6))
        plt.title("Giá trung bình theo loại hàng (query)")
        plt.xlabel("Loại hàng")
        plt.ylabel("Giá trung bình")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(os.path.join(eda_dir, "eda_avg_price_query.png"))
        plt.close()

    print(f"\nĐã lưu toàn bộ kết quả EDA vào: {eda_dir}")


if __name__ == "__main__":
    eda_api()
