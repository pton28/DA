import duckdb
import pandas as pd
from charset_normalizer import from_path
import glob
import logging
import os
import re

SOURCE_DIR = './data/Raw'  # Thư mục chứa file CSV
CLEAN_DIR = './data/Clean'  # Thư mục lưu file CSV đã làm sạch
DATABASE_PATH = './staging/staging.db'     # File path DuckDB cho staging (có thể dùng ':memory:' cho in-memory)
OVERWRITE_TABLES = False

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def safe_read_csv(file_path, **kwargs):
    """
    Đọc CSV an toàn 100% với bất kỳ encoding nào.
    Trả về DataFrame hoặc raise lỗi rõ ràng.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File không tồn tại: {file_path}")

    # 1. Phát hiện encoding chính xác bằng charset-normalizer
    try:
        detection = from_path(file_path, cp_isolation=None)  # Không giới hạn
        best = detection.best()
        if not best:
            raise ValueError("Không phát hiện được encoding")
        detected_encoding = best.encoding
        confidence = best.confidence  # Độ tin cậy (0.0 - 1.0)
        logger.info(f"Phát hiện encoding: {detected_encoding} (độ tin cậy: {confidence:.2f}) cho file: {file_path}")
    except Exception as e:
        logger.warning(f"Không thể phát hiện encoding tự động: {e}. Dùng fallback.")
        detected_encoding = None

    # 2. Danh sách encoding ưu tiên thử (rất rộng)
    encodings_to_try = [
        detected_encoding,           # Ưu tiên phát hiện tự động
        'utf-8', 'utf-8-sig',
        'cp1252', 'windows-1252',
        'latin1', 'iso-8859-1',
        'cp1250', 'cp1251', 'cp1253', 'cp1254', 'cp1255', 'cp1256',
        'gb2312', 'gbk', 'big5',
        'shift-jis', 'euc-jp', 'euc-kr',
        'ascii'
    ]
    # Loại bỏ None và trùng lặp
    encodings_to_try = list(dict.fromkeys([e for e in encodings_to_try if e]))

    # 3. Thử đọc với từng encoding
    for enc in encodings_to_try:
        try:
            df = pd.read_csv(file_path, encoding=enc, low_memory=False, **kwargs)
            logger.info(f"Đọc thành công với encoding: {enc}")
            return df
        except UnicodeDecodeError:
            logger.debug(f"Thất bại với encoding: {enc}")
            continue
        except Exception as e:
            logger.debug(f"Lỗi khác với {enc}: {e}")
            continue

    # 4. Fallback cuối cùng: Đọc bằng 'latin1' (đọc được mọi byte, không crash)
    logger.warning(f"Dùng fallback 'latin1' cho file: {file_path}")
    try:
        df = pd.read_csv(file_path, encoding='latin1', low_memory=False, **kwargs)
        # Cố gắng chuyển về UTF-8 nếu có thể
        df = df.apply(lambda x: x.str.encode('latin1').str.decode('utf-8', errors='replace') if x.dtype == "object" else x)
        return df
    except Exception as e:
        raise RuntimeError(f"Không thể đọc file dù đã thử mọi cách: {file_path} | Lỗi: {e}")

# Hàm Extract
def extract_csv(file_path):
    """Extract: Đọc dữ liệu từ một file CSV."""
    return safe_read_csv(file_path)

# Hàm Transform
def transform_data(df, table_name):
    # Xử lý duplicate data
    initial_row_count = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    final_row_count = len(df)
    duplicates_removed = initial_row_count - final_row_count
    logger.info(f"Bảng '{table_name}': Đã xử lý và loại bỏ {duplicates_removed} bản ghi trùng lặp.")
    # Xử lý missing values

        # Xử lý cho table cleaned_products_api
    if table_name == 'cleaned_products_api':
        if "fetch_time" in df.columns:
            df.sort_values(by="fetch_time", inplace=True)
        if "us_item_id" in df.columns:
            df = df.drop_duplicates(subset=["us_item_id"], keep="last")
        elif "product_id" in df.columns:
            df = df.drop_duplicates(subset=["product_id"], keep="last")

        # Xử lý cho table marketing_data
    elif table_name == 'marketing_data':
        return 
    
        # Xử lý cho table walmart_customers_purchases
    elif table_name == 'walmart_customers_purchases':    
        return
    
        # Xử lý cho table walmart_products
    else:
        # Giữ các cột cần thiết
        filter_columns = ['product_id','product_name','brand','final_price','initial_price','discount','review_count','rating','category_name','root_category_name','available_for_delivery', 'available_for_pickup']
        keep(df, filter_columns)
        # Chuyển kiểu dữ liệu phân loại
        category_features = ['brand', 'category_name', 'root_category_name', 'available_for_delivery', 'available_for_pickup']
        set_category(df, category_features) 
        # Xử lý discount
        df['discount'] = df['discount'].str.replace('$', '', regex=False)
        df['discount'] = df['discount'].astype("float64")

        # Xử lý missing values
        logger.info("Bắt đầu xử lý missing values cho 'initial_price' và 'discount'...")
        # Loại bỏ các sản phẩm có giá trị 'initial_price' rỗng thấp hơn 5%
        columns_to_process = ['initial_price', 'discount']
        df = drop_low_null_products(df, 'root_category_name', columns_to_process)
        # Điền các missing values bằng giá trị trung bình của category
        df = fill_missing_with_category_mean(df, 'root_category_name', columns_to_process)

    return df

# Hàm giữ lại các thuộc tính cần thiết
def keep(df, features):
    cols_to_drop = [col for col in df.columns if col not in features]
    df.drop(columns=cols_to_drop, axis=1, inplace=True)

# Hàm set các biến phân loại về kiểu category
def set_category(df, category_columns):
    for col in category_columns:
        if col in df.columns:
            df[col] = df[col].astype('category')

# Hàm loại bỏ các sản phẩm có giá trị rỗng 5% so với tổng số sản phẩm của catogory đó
def drop_low_null_products(df, root_category_column, value_columns, threshold=0.05):
    if root_category_column not in df.columns or not all(col in df.columns for col in value_columns):
        return df
    categories_to_check = df[root_category_column].unique()

    indices_to_drop = []
    for category in categories_to_check:
        cat_df = df[df[root_category_column] == category]
        total_count = len(cat_df)

        null_rows_mask = cat_df[value_columns].isna().any(axis=1)
        null_rows = cat_df[null_rows_mask]

        missing_count = len(null_rows)
        if missing_count / total_count <= threshold or missing_count == len(cat_df):
            indices_to_drop.extend(null_rows.index.tolist())

    df.drop(indices_to_drop, inplace=True)

    cols_str = ', '.join(value_columns)
    logger.info(f"Đã loại bỏ {len(indices_to_drop)} sản phẩm có giá trị rỗng vượt quá {threshold*100}% trong cột '{cols_str}' theo '{root_category_column}'.")
    return df

# Hàm fill missing values bằng giá trị trung bình của category
def fill_missing_with_category_mean(df, root_category_column, value_columns):
    if root_category_column not in df.columns or not all(col in df.columns for col in value_columns):
        return df
    
    categories_to_check = df[root_category_column].unique()
    for col in value_columns:
        if df[col].isna().any():
            category_means = df.groupby(root_category_column, observed=False)[col].transform('mean')
            df[col] = df[col].fillna(category_means)
    
    logger.info(f"Đã điền giá trị missing trong cột '{', '.join(value_columns)}' bằng giá trị trung bình của category tương ứng.")
    return df

# Hàm lưu DataFrame đã làm sạch
def save_cleaned_data(df, original_filename):
    if not os.path.exists(CLEAN_DIR):
        os.makedirs(CLEAN_DIR)
    
    clean_filename = f"cleaned_{original_filename}"
    clean_path = os.path.join(CLEAN_DIR, clean_filename)
    
    # Lưu file với encoding utf-8
    try:
        df.to_csv(clean_path, index=False, encoding='utf-8')
        logger.info(f"Đã lưu dữ liệu đã làm sạch vào: {clean_path}")
    except Exception as e:
        logger.error(f"Lỗi khi lưu file đã làm sạch {clean_path}: {e}")

# Hàm Load
def run_etl():
    # Kết nối tới DuckDB
    conn = duckdb.connect(database=DATABASE_PATH)
    #B1 . Extract dữ liệu từ CSV
    try:
        csv_files = glob.glob(os.path.join(SOURCE_DIR, '*.csv'))
        if not csv_files:
            raise ValueError("No csv files found.")
        
        for file_path in csv_files:
            file_name = os.path.basename(file_path).split('.')[0]
            df = extract_csv(file_path)

            table_name = re.sub(r"[^0-9a-zA-Z_]", "_", file_name)
            if re.match(r"^[0-9]", table_name):
                table_name = "t_" + table_name

            print(f"ETLing file {file_path} -> table: {table_name}")

            # B2. Transform dữ liệu
            df = transform_data(df, table_name)
            
            # Lưu dữ liệu đã làm sạch
            original_filename = os.path.basename(file_path)
            save_cleaned_data(df, original_filename)

            # Xoá bảng nếu đã tồn tại và overwrite được bật
            if OVERWRITE_TABLES:
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            # B3. Load dữ liệu vào DuckDB
            try:
                conn.register('tmp_df', df)
                conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM tmp_df")
                # Unregister tạm thời để tránh xung đột tên trong vòng lặp tiếp theo
                try:
                    conn.unregister('tmp_df')
                except Exception:
                    pass
            except Exception as e:
                logger.error(f"Failed to write table {table_name} from {file_path}: {e}")
                raise
    except Exception as e:
        print(f"Error during ETL: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_etl()