# Golden Layer (Star Schema)

Golden layer tạo bộ dữ liệu phân tích với 5 dimension + 1 fact và nạp thẳng vào DuckDB.

## Bảng đầu ra
- `DIM_PRODUCT` (product_key, product_id, product_name, brand, category, rating, review_count, source)
- `DIM_CUSTOMER` (customer_key, customer_id, age, age_group, city, gender)
- `DIM_DATE` (date_key, full_date, day, day_name, day_of_week, week_of_year, month, month_name, quarter, year, is_weekend)
- `DIM_PAYMENT` (payment_key, payment_method)
- `DIM_CATEGORY` (category_key, category_name, root_category_name)
- `FACT_SALES` (transaction_id, date_key, customer_key, product_key, payment_key, category_key, purchase_amount, discount_applied, rating, repeat_customer)

## Chạy pipeline
```bash
python pipelines/golden/run_pipeline.py
```
Kết quả:
- CSV chuẩn hóa: `data/Golden/standardized/` (kèm `product_master.csv`)
- Dimension: `data/Golden/dimensions/`
- Fact: `data/Golden/facts/`
- DuckDB: `database/walmart_analytics.db` chứa dim_* và fact_sales

## Kiểm tra khóa/FK
```bash
python pipelines/golden/validate_schema.py
```

## Dùng với Power BI (tránh khóa file)
```python
import duckdb, shutil, tempfile, uuid
from pathlib import Path
src = Path(r"C:\Users\LENOVO\OneDrive\Documents\DA_pipeline\DA\database\walmart_analytics.db")
tmp = Path(tempfile.gettempdir()) / f"walmart_analytics_{uuid.uuid4().hex}.db"
shutil.copy2(src, tmp)
con = duckdb.connect(str(tmp), read_only=True)
DIM_PRODUCT  = con.execute("select * from dim_product").fetchdf()
DIM_CUSTOMER = con.execute("select * from dim_customer").fetchdf()
DIM_DATE     = con.execute("select * from dim_date").fetchdf()
DIM_PAYMENT  = con.execute("select * from dim_payment").fetchdf()
DIM_CATEGORY = con.execute("select * from dim_category").fetchdf()
FACT_SALES   = con.execute("select * from fact_sales").fetchdf()
con.close()
```

## Ghi chú thiết kế
- `product_id` là hash ổn định từ product_name để đồng bộ các nguồn.
- `discount_applied`, `repeat_customer` là 0/1, `purchase_amount`/`rating` là số để Power BI tự nhận measure.
- `DIM_DATE` tự sinh theo khoảng ngày trong dữ liệu mua hàng (mở rộng biên +/-). 
