import pandas as pd
import os
from charset_normalizer import from_path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def safe_read_walmart(file_path):
    """Try several encodings and parser fallbacks to read a messy CSV.

    Returns a DataFrame or raises a RuntimeError with a helpful message.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File does not exist: {file_path}")

    # Try to detect encoding first
    detected = None
    try:
        det = from_path(file_path)
        best = det.best()
        if best:
            detected = best.encoding
    except Exception:
        detected = None

    encodings_to_try = [
        detected,
        'utf-8', 'utf-8-sig',
        'cp1252', 'windows-1252',
        'latin1', 'iso-8859-1',
    ]
    encodings_to_try = [e for e in dict.fromkeys(encodings_to_try) if e]

    # Try reading with the fast C engine first, then fallback to python engine
    last_err = None
    for enc in encodings_to_try:
        try:
            df = pd.read_csv(file_path, encoding=enc, low_memory=False)
            print(f"Read success with encoding={enc} (C engine)")
            return df
        except UnicodeDecodeError as e:
            last_err = e
            # try next encoding
            continue
        except pd.errors.ParserError as e:
            # Tokenization/parsing error with C engine — try python engine with permissive on_bad_lines
            try:
                df = pd.read_csv(file_path, encoding=enc, engine='python', on_bad_lines='warn')
                print(f"Read success with encoding={enc} (python engine, on_bad_lines=warn)")
                return df
            except Exception as e2:
                last_err = e2
                continue
        except Exception as e:
            last_err = e
            continue

    # Final fallback: latin1 + python engine (will not fail on byte values but may produce mangled text)
    try:
        df = pd.read_csv(file_path, encoding='latin1', engine='python', on_bad_lines='warn')
        print("Read success with fallback encoding=latin1 (python engine)")
        # try to coerce text columns back to utf-8 where possible
        for col in df.select_dtypes(include=['object']).columns:
            try:
                df[col] = df[col].astype(str).apply(lambda s: s.encode('latin1').decode('utf-8', errors='replace'))
            except Exception:
                pass
        return df
    except Exception as e:
        raise RuntimeError(f"Cannot read file despite fallbacks: {file_path} | Last error: {last_err or e}")

def transform(df):
    # Xử lý trùng lặp
    initial_row_count = len(df)
    df = df.drop_duplicates()
    duplicates_removed = initial_row_count - len(df)
    logger.info(f"Bảng 'walmart_products': Đã xử lý và loại bỏ {duplicates_removed} bản ghi trùng lặp.")

    # Giữ các cột cần thiết
    filter_columns = ['product_id','product_name','brand','final_price','initial_price','discount','review_count','rating','category_name','root_category_name','available_for_delivery', 'available_for_pickup']
    keep(df, filter_columns)
    
    # Chuyển các biến phân loại về kiểu category
    category_features = ['brand', 'category_name', 'root_category_name', 'available_for_delivery', 'available_for_pickup']
    set_category(df, category_features) 

    # Xử lý cột discount
    df['discount'] = df['discount'].str.replace('$', '', regex=False)
    df['discount'] = df['discount'].astype("float64")

    # Xử lý missing values
    logger.info("Bắt đầu xử lý missing values cho 'initial_price' và 'discount'...")
    # Loại bỏ các sản phẩm có giá trị 'initial_price' rỗng thấp hơn 5%
    columns_to_process = ['initial_price', 'discount']
    df = drop_low_null_products(df, 'root_category_name', columns_to_process)
    # Điền các missing values bằng giá trị trung bình của category
    df = fill_missing_with_category_mean(df, 'root_category_name', columns_to_process)

    df.reset_index(drop=True, inplace=True)

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

# Hàm loại bỏ các sản phẩm có giá trị 'initial_price' và 'discount' rỗng 5% so với tổng số sản phẩm của catogory đó
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

if __name__ == '__main__':
    path = os.path.join('data\\Raw', 'walmart_products.csv')
    try:
        df = safe_read_walmart(path)
        print(df.head())
        df = transform(df)
        df.info()
    except Exception as e:
        print(f"Error reading '{path}': {e}")