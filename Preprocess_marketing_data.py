"""
===================================================================
TIỀN XỬ LÍ DỮ LIỆU MARKETING - PHIÊN BẢN HOÀN CHỈNH
===================================================================
Kết hợp:
1. Fix cấu trúc CSV (29 cột data → 28 cột header)
2. Tiền xử lí dữ liệu đỉnh cao với ML algorithms
   - KNN Imputation (<5% missing)
   - Iterative Imputation/MICE (5-30% missing)
   - Median/Mode (>30% missing)
   - IQR Outlier Detection & Handling
   - Text Cleaning & Normalization
   - Feature Engineering
===================================================================
"""

import pandas as pd
import numpy as np
import csv
import json
import re
from datetime import datetime
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import KNNImputer, IterativeImputer
from scipy import stats

print("="*80)
print(" TIỀN XỬ LÍ DỮ LIỆU MARKETING - PHIÊN BẢN HOÀN CHỈNH")
print("="*80)

# ==================== BƯỚC 1: FIX CẤU TRÚC CSV ====================
print("\n BƯỚC 1: ĐỌC VÀ FIX CẤU TRÚC CSV...")

rows = []
with open('marketing_data.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    header = next(reader)
    
    print(f"   Header: {len(header)} cột")
    
    for i, row in enumerate(reader):
        # Fix: Data có 29 cột, header có 28 cột → bỏ cột cuối
        if len(row) > len(header):
            row = row[:len(header)]
        elif len(row) < len(header):
            row = row + ['NA'] * (len(header) - len(row))
        
        rows.append(row)
        
        if (i+1) % 10000 == 0:
            print(f"   Đang đọc: {i+1:,} dòng...")

print(f"✅ Đọc xong: {len(rows):,} dòng")

# Tạo DataFrame
df = pd.DataFrame(rows, columns=header)
print(f"📊 DataFrame gốc: {df.shape[0]:,} dòng × {df.shape[1]} cột")

# ==================== BƯỚC 2: PARSE NUMERIC COLUMNS ====================
print("\n🔧 BƯỚC 2: PARSE NUMERIC COLUMNS...")

def parse_numeric(value):
    """Parse numeric values, xử lí NA/null/currency"""
    if pd.isna(value) or value in ['NA', 'na', 'N/A', '', 'NULL', 'null']:
        return np.nan
    try:
        # Remove currency symbols và commas
        cleaned = str(value).replace(',', '').replace('$', '').replace('€', '').replace('£', '')
        return float(cleaned)
    except:
        return np.nan

# Parse các cột numeric
numeric_columns = {
    'Price': 'Price',
    'Monthly Price': 'Monthly Price', 
    'Num Of Reviews': 'Num Of Reviews',
    'Average Rating': 'Average Rating',
    'Number Of Ratings': 'Number Of Ratings',
    'Five Star': 'Five Star',
    'Four Star': 'Four Star',
    'Three Star': 'Three Star',
    'Two Star': 'Two Star',
    'One Star': 'One Star'
}

for col in numeric_columns:
    if col in df.columns:
        df[col] = df[col].apply(parse_numeric)
        null_pct = df[col].isna().sum() / len(df) * 100
        print(f"   ✅ {col}: {df[col].notna().sum():,} values ({100-null_pct:.2f}% complete)")

# ==================== BƯỚC 3: CLEAN TEXT COLUMNS ====================
print("\n📝 BƯỚC 3: CLEAN TEXT COLUMNS...")

def clean_text(text):
    """Làm sạch text: normalize whitespace, handle NA"""
    if pd.isna(text) or text in ['NA', 'na', 'N/A', '', 'NULL', 'null']:
        return 'Unknown'
    
    text = str(text).strip()
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Remove special characters ở đầu/cuối
    text = re.sub(r'^[^\w]+|[^\w]+$', '', text)
    
    return text if text else 'Unknown'

text_columns = ['Title', 'Manufacturer', 'Model Name', 'Carrier', 
                'Color Category', 'Internal Memory', 'Screen Size', 'Specifications']

for col in text_columns:
    if col in df.columns:
        df[col] = df[col].apply(clean_text)

print(f"✅ Làm sạch {len([c for c in text_columns if c in df.columns])} cột text")

# ==================== BƯỚC 4: PARSE BOOLEAN & DATETIME ====================
print("\n📅 BƯỚC 4: PARSE BOOLEAN & DATETIME...")

# Parse boolean columns
bool_map = {
    'true': True, 'True': True, 'TRUE': True, True: True, 't': True, 'T': True, '1': True,
    'false': False, 'False': False, 'FALSE': False, False: False, 'f': False, 'F': False, '0': False
}

bool_columns = ['Stock', 'Discontinued', 'Broken Link']
for col in bool_columns:
    if col in df.columns:
        df[col] = df[col].map(bool_map)
        df[col] = df[col].fillna(False)

# Parse datetime
if 'Crawl Timestamp' in df.columns:
    df['Crawl Timestamp'] = pd.to_datetime(df['Crawl Timestamp'], errors='coerce')
    df['crawl_year'] = df['Crawl Timestamp'].dt.year
    df['crawl_month'] = df['Crawl Timestamp'].dt.month
    df['crawl_day'] = df['Crawl Timestamp'].dt.day
    df['crawl_dayofweek'] = df['Crawl Timestamp'].dt.dayofweek

print("✅ Parse boolean & datetime hoàn thành")

# ==================== BƯỚC 5: PHÂN TÍCH MISSING VALUES ====================
print("\n📊 BƯỚC 5: PHÂN TÍCH MISSING VALUES...")

missing_analysis = []
for col in df.columns:
    null_count = df[col].isna().sum()
    null_pct = null_count / len(df) * 100
    if null_pct > 0:
        missing_analysis.append({
            'column': col,
            'missing_count': null_count,
            'missing_pct': null_pct
        })

missing_df = pd.DataFrame(missing_analysis).sort_values('missing_pct', ascending=False)
print("\nTop 15 cột có missing values:")
print(missing_df.head(15).to_string(index=False))

# ==================== BƯỚC 6: IMPUTE MISSING VALUES ====================
print("\n🎯 BƯỚC 6: IMPUTE MISSING VALUES VỚI ML ALGORITHMS...")

# Xác định cột cần impute (numeric columns với 0-95% missing)
impute_cols = []
for col in ['Price', 'Monthly Price', 'Average Rating', 'Num Of Reviews', 
            'Number Of Ratings', 'Five Star', 'Four Star', 'Three Star', 
            'Two Star', 'One Star']:
    if col in df.columns:
        missing_pct = df[col].isna().sum() / len(df) * 100
        if 0 < missing_pct < 95:
            impute_cols.append(col)

print(f"\nCột sẽ impute: {impute_cols}")

def smart_impute_numeric(df, columns):
    """
    Impute numeric columns với strategy thông minh:
    - <5% missing: KNN Imputation
    - 5-30% missing: Iterative Imputation (MICE)
    - >30% missing: Median Imputation
    """
    df_result = df.copy()
    
    for col in columns:
        missing_pct = df[col].isna().sum() / len(df) * 100
        
        if missing_pct == 0:
            continue
            
        print(f"\n   • {col} ({missing_pct:.2f}% missing):")
        
        if missing_pct < 5:
            # KNN Imputation
            print(f"      → Sử dụng KNN Imputation (n_neighbors=5)")
            
            # Tìm các cột tương quan để làm features
            numeric_df = df[columns].select_dtypes(include=[np.number])
            
            if col in numeric_df.columns and numeric_df[col].notna().sum() > 0:
                # Tính correlation
                corr = numeric_df.corr()[col].abs().sort_values(ascending=False)
                # Lấy top 6 cột (bao gồm chính nó)
                feature_cols = [c for c in corr.head(6).index.tolist() if c in df.columns]
                
                if len(feature_cols) > 1:
                    imputer = KNNImputer(n_neighbors=min(5, df[feature_cols].notna().all(axis=1).sum()), 
                                        weights='distance')
                    df_result[feature_cols] = imputer.fit_transform(df[feature_cols])
                else:
                    # Fallback to median
                    df_result[col] = df_result[col].fillna(df[col].median())
            
        elif missing_pct < 30:
            # Iterative Imputation (MICE)
            print(f"      → Sử dụng Iterative Imputation (MICE, max_iter=10)")
            
            numeric_df = df[columns].select_dtypes(include=[np.number])
            
            if len(numeric_df.columns) > 1:
                imputer = IterativeImputer(max_iter=10, random_state=42)
                df_result[numeric_df.columns] = imputer.fit_transform(numeric_df)
            else:
                df_result[col] = df_result[col].fillna(df[col].median())
                
        else:
            # Median Imputation
            print(f"      → Sử dụng Median Imputation")
            median_val = df[col].median()
            if pd.isna(median_val):
                median_val = 0
            df_result[col] = df_result[col].fillna(median_val)
    
    return df_result

if impute_cols:
    df = smart_impute_numeric(df, impute_cols)
    print("\n✅ Imputation hoàn thành")

# ==================== BƯỚC 7: XỬ LÍ OUTLIERS ====================
print("\n🔍 BƯỚC 7: XỬ LÍ OUTLIERS VỚI IQR METHOD...")

def detect_and_handle_outliers(df, column):
    """
    Phát hiện và xử lí outliers với IQR method
    Strategy:
    - <5% outliers: Winsorization (1st-99th percentile)
    - 5-15% outliers: IQR Capping
    - >15% outliers: Giữ nguyên
    """
    if df[column].dtype not in ['float64', 'int64']:
        return df, 0
    
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    outliers = ((df[column] < lower_bound) | (df[column] > upper_bound))
    outlier_count = outliers.sum()
    outlier_pct = outlier_count / len(df) * 100
    
    if outlier_count == 0:
        return df, 0
    
    print(f"   • {column}: {outlier_count:,} outliers ({outlier_pct:.2f}%)")
    
    if outlier_pct < 5:
        # Winsorization
        p1 = df[column].quantile(0.01)
        p99 = df[column].quantile(0.99)
        df[column] = df[column].clip(lower=p1, upper=p99)
        print(f"      → Winsorized: [{p1:.2f}, {p99:.2f}]")
    elif outlier_pct < 15:
        # IQR Capping
        df[column] = df[column].clip(lower=lower_bound, upper=upper_bound)
        print(f"      → IQR Capped: [{lower_bound:.2f}, {upper_bound:.2f}]")
    else:
        print(f"      → Giữ nguyên (>15% outliers)")
    
    return df, outlier_count

outlier_cols = ['Price', 'Monthly Price', 'Average Rating', 'Num Of Reviews', 
                'Number Of Ratings']
total_outliers = 0

for col in outlier_cols:
    if col in df.columns:
        df, count = detect_and_handle_outliers(df, col)
        total_outliers += count

print(f"\n✅ Xử lí {total_outliers:,} outliers tổng cộng")

# ==================== BƯỚC 8: FEATURE ENGINEERING ====================
print("\n🎨 BƯỚC 8: FEATURE ENGINEERING...")

features_created = []

# 1. Total star ratings
if all(col in df.columns for col in ['Five Star', 'Four Star', 'Three Star', 'Two Star', 'One Star']):
    df['total_star_ratings'] = (
        df['Five Star'] + df['Four Star'] + 
        df['Three Star'] + df['Two Star'] + df['One Star']
    )
    features_created.append('total_star_ratings')

# 2. Has reviews flag
if 'Num Of Reviews' in df.columns:
    df['has_reviews'] = (df['Num Of Reviews'] > 0).astype(int)
    features_created.append('has_reviews')

# 3. Price range category
if 'Price' in df.columns:
    df['price_range'] = pd.cut(
        df['Price'],
        bins=[0, 50, 100, 200, 500, float('inf')],
        labels=['Budget', 'Mid', 'Premium', 'High-end', 'Luxury']
    )
    features_created.append('price_range')

# 4. Rating quality
if 'Average Rating' in df.columns:
    df['rating_quality'] = pd.cut(
        df['Average Rating'],
        bins=[0, 2, 3, 4, 5],
        labels=['Poor', 'Fair', 'Good', 'Excellent']
    )
    features_created.append('rating_quality')

# 5. Review density (reviews per rating)
if 'Num Of Reviews' in df.columns and 'Number Of Ratings' in df.columns:
    df['review_density'] = df['Num Of Reviews'] / (df['Number Of Ratings'] + 1)
    features_created.append('review_density')

# 6. Price per rating point
if 'Price' in df.columns and 'Average Rating' in df.columns:
    df['price_per_rating'] = df['Price'] / (df['Average Rating'] + 0.1)
    features_created.append('price_per_rating')

print(f"✅ Tạo {len(features_created)} features mới:")
for feat in features_created:
    print(f"   - {feat}")

# ==================== BƯỚC 9: DROP LOW-VALUE COLUMNS ====================
print("\n🗑️  BƯỚC 9: DROP CÁC CỘT KHÔNG GIÁ TRỊ...")

drop_candidates = []
for col in df.columns:
    null_pct = df[col].isna().sum() / len(df) * 100
    unique_count = df[col].nunique()
    
    should_drop = False
    reason = ""
    
    # Drop nếu >95% missing
    if null_pct > 95:
        should_drop = True
        reason = f"{null_pct:.1f}% missing"
    # Drop nếu chỉ 1 giá trị unique
    elif unique_count == 1:
        should_drop = True
        reason = "Only 1 unique value"
    # Drop ID columns không cần thiết
    elif col in ['Uniq Id', 'Pageurl'] and unique_count > len(df) * 0.95:
        should_drop = True
        reason = "ID column không cần cho analysis"
    
    if should_drop:
        drop_candidates.append({
            'column': col,
            'reason': reason,
            'null_pct': null_pct,
            'unique': unique_count
        })

if drop_candidates:
    drop_cols = [item['column'] for item in drop_candidates]
    print(f"   Dropping {len(drop_cols)} columns:")
    for item in drop_candidates:
        print(f"      - {item['column']}: {item['reason']}")
    
    df = df.drop(columns=drop_cols)
else:
    print("   ✅ Không có cột nào cần drop")

# ==================== BƯỚC 10: NORMALIZE COLUMN NAMES ====================
print("\n📋 BƯỚC 10: NORMALIZE COLUMN NAMES...")

df.columns = (df.columns
              .str.strip()
              .str.lower()
              .str.replace(' ', '_')
              .str.replace(r'[^\w]', '', regex=True))

print(f"✅ Normalize {len(df.columns)} column names")

# ==================== BƯỚC 11: LƯU KẾT QUẢ ====================
print("\n💾 BƯỚC 11: LƯU KẾT QUẢ...")

# Lưu clean data
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_file = f'marketing_data_clean_{timestamp}.csv'
df.to_csv(output_file, index=False, encoding='utf-8-sig')
print(f"✅ Lưu file: {output_file}")

# Tạo schema JSON
schema = {
    'filename': output_file,
    'created_at': datetime.now().isoformat(),
    'original_rows': 29991,
    'final_rows': len(df),
    'original_columns': 28,
    'final_columns': len(df.columns),
    'data_retention_rate': f"{len(df)/29991*100:.2f}%",
    'column_info': {}
}

for col in df.columns:
    col_info = {
        'dtype': str(df[col].dtype),
        'null_count': int(df[col].isna().sum()),
        'null_pct': float(df[col].isna().sum() / len(df) * 100),
        'unique_count': int(df[col].nunique())
    }
    
    # Add sample values
    if df[col].dtype in ['object', 'category', 'bool']:
        col_info['sample_values'] = df[col].dropna().head(5).tolist()
    elif 'datetime' in str(df[col].dtype):
        col_info['min'] = str(df[col].min()) if pd.notna(df[col].min()) else None
        col_info['max'] = str(df[col].max()) if pd.notna(df[col].max()) else None
    else:
        try:
            col_info['min'] = float(df[col].min()) if pd.notna(df[col].min()) else None
            col_info['max'] = float(df[col].max()) if pd.notna(df[col].max()) else None
            col_info['mean'] = float(df[col].mean()) if pd.notna(df[col].mean()) else None
        except:
            col_info['sample_values'] = df[col].dropna().head(5).tolist()
    
    schema['column_info'][col] = col_info

with open('marketing_data_schema.json', 'w', encoding='utf-8') as f:
    json.dump(schema, f, indent=2, ensure_ascii=False)
print("✅ Lưu schema: marketing_data_schema.json")

# Tạo báo cáo chi tiết
report = f"""
{'='*80}
BÁO CÁO TIỀN XỬ LÍ DỮ LIỆU MARKETING - PHIÊN BẢN HOÀN CHỈNH
{'='*80}
Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📊 TỔNG QUAN:
   Input:  29,991 dòng × 28 cột
   Output: {len(df):,} dòng × {len(df.columns)} cột
   Tỉ lệ giữ lại: {len(df)/29991*100:.2f}%

🔧 CÁC BƯỚC ĐÃ THỰC HIỆN:
   1. ✅ Fix cấu trúc CSV (29 cột data → 28 cột header)
   2. ✅ Parse numeric columns: Price, Ratings, Reviews
   3. ✅ Clean text columns: Title, Manufacturer, Model Name...
   4. ✅ Parse boolean & datetime columns
   5. ✅ Phân tích missing values
   6. ✅ Impute missing values:
      - KNN Imputation (<5% missing)
      - Iterative/MICE (5-30% missing)
      - Median (>30% missing)
   7. ✅ Xử lí outliers với IQR method
   8. ✅ Feature engineering: {len(features_created)} features mới
   9. ✅ Drop low-value columns
   10. ✅ Normalize column names
   11. ✅ Export CSV + JSON schema

📈 CÁC CỘT QUAN TRỌNG:
"""

important_cols = ['price', 'average_rating', 'num_of_reviews', 
                  'five_star', 'four_star', 'three_star', 'two_star', 'one_star',
                  'model_name', 'manufacturer', 'title']

for col in important_cols:
    if col in df.columns:
        null_pct = df[col].isna().sum() / len(df) * 100
        unique_count = df[col].nunique()
        report += f"   - {col}: {df[col].dtype}, {null_pct:.2f}% missing, {unique_count:,} unique values\n"

report += f"\n🎨 FEATURES MỚI:\n"
for feat in features_created:
    if feat in df.columns:
        report += f"   - {feat}\n"

report += f"\n{'='*80}\n"
report += "✅ TIỀN XỬ LÍ HOÀN TẤT!\n"
report += f"{'='*80}\n"

with open('data_cleaning_report.txt', 'w', encoding='utf-8') as f:
    f.write(report)

print(report)

print("\n" + "="*80)
print("🎉 HOÀN THÀNH TOÀN BỘ QUY TRÌNH!")
print("📁 Files output:")
print(f"   - {output_file}")
print("   - marketing_data_schema.json")
print("   - data_cleaning_report.txt")
print("="*80)
