"""
transforming.py
Module for Transform stage of ETL.
Includes all transformation logic exactly preserved from your original code,
but reorganized cleanly into separate functions and TRANSFORM_MAP.
"""

import re
import logging
import numpy as np
import pandas as pd
from sklearn.experimental import enable_iterative_imputer  # noqa: F401
from sklearn.impute import KNNImputer, IterativeImputer

logger = logging.getLogger(__name__)

pd.set_option('future.no_silent_downcasting', True)
# -----------------------------------------------------------------------------
# === Generic helpers (kept 100% from original logic) ===
# -----------------------------------------------------------------------------

def parse_numeric(value):
    try:
        if pd.isna(value):
            return np.nan
        sval = str(value).strip()
        if sval.lower() in ['na', 'n/a', 'null', 'none', '']:
            return np.nan
        cleaned = re.sub(r"[^0-9\-\.]", "", sval)
        if cleaned in ['', '-', '.']:
            return np.nan
        return float(cleaned)
    except Exception:
        return np.nan


def clean_text(text):
    if pd.isna(text) or text in ['NA', 'na', 'N/A', '', 'NULL', 'null']:
        return 'Unknown'
    text = str(text).strip()
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'^[^\w]+|[^\w]+$', '', text)
    return text if text else 'Unknown'


def smart_impute_numeric(df, columns):
    df_result = df.copy()
    cols_to_knn = []
    cols_to_mice = []
    cols_to_median = []

    for col in columns:
        missing_pct = df_result[col].isna().sum() / len(df_result) * 100
        if 0 < missing_pct < 5:
            cols_to_knn.append(col)
        elif 5 <= missing_pct < 30:
            cols_to_mice.append(col)
        elif missing_pct >= 30:
            cols_to_median.append(col)

    # KNN
    if cols_to_knn:
        try:
            imputer_knn = KNNImputer(n_neighbors=5, weights='distance')
            df_result[cols_to_knn] = imputer_knn.fit_transform(df_result[cols_to_knn])
        except Exception:
            cols_to_median.extend(cols_to_knn)

    # MICE
    if cols_to_mice:
        try:
            imputer = IterativeImputer(max_iter=10, random_state=42)
            df_result[cols_to_mice] = imputer.fit_transform(df_result[cols_to_mice])
        except Exception:
            cols_to_median.extend(cols_to_mice)

    # Median
    for col in cols_to_median:
        median_val = df_result[col].median()
        if pd.isna(median_val):
            median_val = 0
        df_result[col] = df_result[col].fillna(median_val)

    return df_result


def detect_and_handle_outliers(df, columns_to_check):
    for column in columns_to_check:
        if column not in df.columns:
            continue
        if not pd.api.types.is_numeric_dtype(df[column].dtype):
            continue

        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        outliers = ((df[column] < lower_bound) | (df[column] > upper_bound))
        outlier_count = outliers.sum()
        if outlier_count == 0:
            continue

        outlier_pct = outlier_count / len(df) * 100
        logger.info(f"   • {column}: {outlier_count:,} outliers ({outlier_pct:.2f}%)")

        if outlier_pct < 5:
            p1 = df[column].quantile(0.01)
            p99 = df[column].quantile(0.99)
            df[column] = df[column].clip(lower=p1, upper=p99)
        elif outlier_pct < 15:
            df[column] = df[column].clip(lower=lower_bound, upper=upper_bound)

    return df


def feature_engineering(df):
    df_featured = df.copy()

    if all(col in df_featured.columns for col in ['Five Star', 'Four Star', 'Three Star', 'Two Star', 'One Star']):
        df_featured['total_star_ratings'] = (
            df_featured['Five Star'] + df_featured['Four Star'] + df_featured['Three Star'] +
            df_featured['Two Star'] + df_featured['One Star']
        )

    if 'Num Of Reviews' in df_featured.columns:
        df_featured['has_reviews'] = (df_featured['Num Of Reviews'] > 0).astype(int)

    if 'Price' in df_featured.columns:
        df_featured['price_range'] = pd.cut(
            df_featured['Price'].astype(float),
            bins=[0, 50, 100, 200, 500, float('inf')],
            labels=['Budget', 'Mid', 'Premium', 'High-end', 'Luxury']
        )

    if 'Average Rating' in df_featured.columns:
        df_featured['rating_quality'] = pd.cut(
            df_featured['Average Rating'].astype(float),
            bins=[0, 2, 3, 4, 5],
            labels=['Poor', 'Fair', 'Good', 'Excellent']
        )

    if 'Num Of Reviews' in df_featured.columns and 'Number Of Ratings' in df_featured.columns:
        df_featured['review_density'] = (
            df_featured['Num Of Reviews'] / (df_featured['Number Of Ratings'] + 1)
        )

    if 'Price' in df_featured.columns and 'Average Rating' in df_featured.columns:
        df_featured['price_per_rating'] = (
            df_featured['Price'].astype(float) /
            (df_featured['Average Rating'].astype(float) + 0.1)
        )

    return df_featured


def drop_low_value_columns(df):
    drop_candidates = []
    for col in df.columns:
        null_pct = df[col].isna().sum() / len(df) * 100
        unique_count = df[col].nunique()

        should_drop = False
        if null_pct > 95:
            should_drop = True
        elif unique_count == 1:
            should_drop = True
        elif col in ['Uniq Id', 'Pageurl'] and unique_count > len(df) * 0.95:
            should_drop = True

        if should_drop:
            drop_candidates.append(col)

    if drop_candidates:
        df = df.drop(columns=drop_candidates)

    return df


def normalize_column_names(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace(r'[^\w]', '', regex=True)
    )
    return df

# -----------------------------------------------------------------------------
# === Logic from original transform functions ===
# -----------------------------------------------------------------------------

def drop_low_null_products(df, root_category_column, value_columns, threshold=0.05):
    if root_category_column not in df.columns:
        return df
    if not all(col in df.columns for col in value_columns):
        return df

    categories_to_check = df[root_category_column].unique()
    indices_to_drop = []

    for category in categories_to_check:
        cat_df = df[df[root_category_column] == category]
        total_count = len(cat_df)
        null_rows_mask = cat_df[value_columns].isna().any(axis=1)
        missing_count = null_rows_mask.sum()

        if missing_count / total_count <= threshold or missing_count == total_count:
            indices_to_drop.extend(cat_df[null_rows_mask].index.tolist())

    df = df.drop(indices_to_drop)
    logger.info(f"Đã loại bỏ {len(indices_to_drop)} sản phẩm rỗng vượt ngưỡng theo category")
    return df


def fill_missing_with_category_mean(df, root_category_column, value_columns):
    if root_category_column not in df.columns:
        return df
    for col in value_columns:
        if col not in df.columns:
            return df

    for col in value_columns:
        df[col] = df.groupby(root_category_column, observed=False)[col].transform(
            lambda g: g.fillna(g.mean())
        )

    logger.info(f"Đã điền missing bằng mean theo category: {value_columns}")
    return df

# -----------------------------------------------------------------------------
# === Transform per table (kept same logic) ===
# -----------------------------------------------------------------------------

def transform_cleaned_products_api(df):
    df = df.copy()
    if 'fetch_time' in df.columns:
        df = df.sort_values(by='fetch_time')
    if 'us_item_id' in df.columns:
        df = df.drop_duplicates(subset=['us_item_id'], keep='last')
    elif 'product_id' in df.columns:
        df = df.drop_duplicates(subset=['product_id'], keep='last')
    df.reset_index(drop=True, inplace=True)
    return df


def transform_marketing_data(df):
    df = df.copy()

    numeric_columns = [
        'Price', 'Monthly Price', 'Num Of Reviews',
        'Average Rating', 'Number Of Ratings',
        'Five Star', 'Four Star', 'Three Star', 'Two Star', 'One Star'
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].apply(parse_numeric)

    text_columns = [
        'Title', 'Manufacturer', 'Model Name', 'Carrier',
        'Color Category', 'Internal Memory', 'Screen Size', 'Specifications'
    ]
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)

    bool_columns = ['Stock', 'Discontinued', 'Broken Link']
    bool_map = {
        'true': True, 'false': False, '1': True, '0': False,
        True: True, False: False
    }
    for col in bool_columns:
        if col in df.columns:
            df[col] = df[col].map(bool_map).fillna(False).infer_objects(copy=False).astype(bool)

    if 'Crawl Timestamp' in df.columns:
        time_format = '%Y-%m-%d %H:%M:%S %z'
        df['Crawl Timestamp'] = pd.to_datetime(df['Crawl Timestamp'], format=time_format, errors='coerce')
        df['crawl_year'] = df['Crawl Timestamp'].dt.year
        df['crawl_month'] = df['Crawl Timestamp'].dt.month
        df['crawl_day'] = df['Crawl Timestamp'].dt.day
        df['crawl_dayofweek'] = df['Crawl Timestamp'].dt.dayofweek

    numeric_cols_to_check = [
        'Price', 'Monthly Price', 'Average Rating',
        'Num Of Reviews', 'Number Of Ratings',
        'Five Star', 'Four Star', 'Three Star', 'Two Star', 'One Star'
    ]
    df = process_missing_values(df, numeric_cols_to_check)

    df = detect_and_handle_outliers(df, [
        'Price', 'Monthly Price', 'Average Rating',
        'Num Of Reviews', 'Number Of Ratings'
    ])

    df = feature_engineering(df)
    df = drop_low_value_columns(df)
    df = normalize_column_names(df)

    return df


def process_missing_values(df, numeric_cols_to_check):
    impute_cols = []
    for col in numeric_cols_to_check:
        if col in df.columns:
            missing_pct = df[col].isna().sum() / len(df) * 100
            if 0 < missing_pct < 95:
                impute_cols.append(col)

    if impute_cols:
        df = smart_impute_numeric(df, impute_cols)
    return df


def transform_walmart_customers_purchases(df):
    df = df.copy()

    if 'Purchase_Date' in df.columns:
        df['Purchase_Date'] = pd.to_datetime(df['Purchase_Date'], errors='coerce')
        df['Year'] = df['Purchase_Date'].dt.year
        df['Month'] = df['Purchase_Date'].dt.month
        df['DayOfWeek'] = df['Purchase_Date'].dt.day_name()

    df.reset_index(drop=True, inplace=True)
    return df


def transform_walmart_products(df):
    df = df.copy()

    filter_columns = [
        'product_id','product_name','brand','final_price','initial_price','discount',
        'review_count','rating','category_name','root_category_name',
        'available_for_delivery', 'available_for_pickup'
    ]
    keep_cols = [col for col in filter_columns if col in df.columns]
    df = df[keep_cols]

    category_features = [
        'brand', 'category_name', 'root_category_name',
        'available_for_delivery', 'available_for_pickup'
    ]
    for col in category_features:
        if col in df.columns:
            df[col] = df[col].astype('category')

    if 'discount' in df.columns:
        df['discount'] = df['discount'].astype(str).str.replace('$', '', regex=False)
        df['discount'] = df['discount'].apply(parse_numeric)

    columns_to_process = ['initial_price', 'discount']

    df = drop_low_null_products(df, 'root_category_name', columns_to_process)
    df = fill_missing_with_category_mean(df, 'root_category_name', columns_to_process)

    return df

# -----------------------------------------------------------------------------
# === TRANSFORM MAP (exactly as requested) ===
# -----------------------------------------------------------------------------

TRANSFORM_MAP = {
    'cleaned_products_api': transform_cleaned_products_api,
    'marketing_data': transform_marketing_data,
    'walmart_customers_purchases': transform_walmart_customers_purchases,
    'walmart_products': transform_walmart_products,
}

# -----------------------------------------------------------------------------
# === Transform entrypoint ===
# -----------------------------------------------------------------------------

def transform_data(df, table_name):
    logger.info(f"[TRANSFORM] Start transform for table: {table_name}")

    # Remove duplicates
    initial = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    dedup = initial - len(df)
    logger.info(f"[TRANSFORM] Removed {dedup} duplicates")

    if table_name in TRANSFORM_MAP:
        return TRANSFORM_MAP[table_name](df)

    # Default normalization if no specific function exists
    return normalize_column_names(df)
