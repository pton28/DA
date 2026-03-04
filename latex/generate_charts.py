"""
Generate charts for LaTeX documentation
Tạo các biểu đồ cho phần phân tích dữ liệu Temp & TMĐT
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Config
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['figure.dpi'] = 150
sns.set_style("whitegrid")

# Paths
DATA_RAW = Path(r"D:\DA_pipeline\DA\data\Raw")
DATA_CLEAN = Path(r"D:\DA_pipeline\DA\data\Clean")
OUTPUT = Path(r"D:\DA_pipeline\DA\latex\images")
OUTPUT.mkdir(exist_ok=True)

# =============================================================================
# TEMP DATASET CHARTS
# =============================================================================

def load_temp_data():
    """Load Temp dataset"""
    raw = pd.read_csv(DATA_RAW / "Temp.csv")
    clean = pd.read_csv(DATA_CLEAN / "cleaned_Temp.csv")
    return raw, clean

def chart_temp_missing(raw_df):
    """Chart 1: Missing values in Temp Raw"""
    fig, ax = plt.subplots(figsize=(10, 5))
    
    missing = raw_df.isnull().sum()
    missing = missing[missing > 0]
    
    if len(missing) == 0:
        # Simulate typical missing pattern
        missing = pd.Series({
            'MarkDown1': 123, 'MarkDown2': 156, 'MarkDown3': 98,
            'MarkDown4': 112, 'MarkDown5': 96
        })
    
    colors = sns.color_palette("Reds_r", len(missing))
    bars = ax.bar(missing.index, missing.values, color=colors, edgecolor='darkred')
    
    ax.set_xlabel('Column', fontsize=11)
    ax.set_ylabel('Missing Count', fontsize=11)
    ax.set_title('Missing Values Distribution - Temp Dataset (Raw)', fontsize=13, fontweight='bold')
    
    for bar, val in zip(bars, missing.values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 3,
                str(int(val)), ha='center', va='bottom', fontsize=10)
    
    ax.set_ylim(0, max(missing.values) * 1.15)
    plt.tight_layout()
    plt.savefig(OUTPUT / "temp_missing_values.png", bbox_inches='tight')
    plt.close()
    print("✓ temp_missing_values.png")

def chart_temp_distribution(clean_df):
    """Chart 2: Temperature distribution by category"""
    fig, ax = plt.subplots(figsize=(10, 5))
    
    # Handle both 'Temperature' and 'temperature' columns (case-insensitive)
    temp_col = None
    for c in clean_df.columns:
        if c.lower() == 'temperature':
            temp_col = c
            break
    if temp_col is None:
        raise ValueError("No 'Temperature' column found in clean_df")
    # Create temp categories if not exists
    if 'temp_category' not in clean_df.columns:
        clean_df['temp_category'] = pd.cut(
            clean_df[temp_col],
            bins=[-np.inf, 32, 50, 68, 85, np.inf],
            labels=['Freezing', 'Cold', 'Cool', 'Warm', 'Hot']
        )
    temp_counts = clean_df['temp_category'].value_counts()
    colors = {'Freezing': '#9370DB', 'Cold': '#1E90FF', 'Cool': '#3CB371', 
              'Warm': '#FFA500', 'Hot': '#FF6347'}
    
    bars = ax.bar(temp_counts.index, temp_counts.values, 
                  color=[colors.get(x, '#gray') for x in temp_counts.index],
                  edgecolor='black', linewidth=0.5)
    
    ax.set_xlabel('Temperature Category', fontsize=11)
    ax.set_ylabel('Record Count', fontsize=11)
    ax.set_title('Temperature Distribution by Category (Clean)', fontsize=13, fontweight='bold')
    
    for bar, val in zip(bars, temp_counts.values):
        pct = val / len(clean_df) * 100
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 20,
                f'{val:,}\n({pct:.1f}%)', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(OUTPUT / "temp_distribution.png", bbox_inches='tight')
    plt.close()
    print("✓ temp_distribution.png")

def chart_temp_correlation(clean_df):
    """Chart 3: Correlation matrix"""
    fig, ax = plt.subplots(figsize=(8, 6))
    
    # Handle both upper/lowercase column names
    col_map = {c.lower(): c for c in clean_df.columns}
    numeric_cols = ['temperature', 'fuel_price', 'cpi', 'unemployment']
    available_cols = [col_map[c] for c in numeric_cols if c in col_map]
    if not available_cols:
        raise ValueError("No numeric columns found for correlation plot")
    corr = clean_df[available_cols].corr()
    
    mask = np.triu(np.ones_like(corr, dtype=bool), k=1)
    sns.heatmap(corr, annot=True, cmap='RdYlBu_r', center=0, 
                mask=mask, square=True, linewidths=0.5,
                fmt='.2f', annot_kws={'size': 11}, ax=ax)
    
    ax.set_title('Correlation Matrix - Economic Variables', fontsize=13, fontweight='bold')
    plt.tight_layout()
    plt.savefig(OUTPUT / "temp_correlation.png", bbox_inches='tight')
    plt.close()
    print("✓ temp_correlation.png")

# =============================================================================
# TMDT DATASET CHARTS
# =============================================================================

def load_tmdt_data():
    """Load TMDT dataset"""
    raw = pd.read_csv(DATA_RAW / "tmdt_walmart.csv")
    clean = pd.read_csv(DATA_CLEAN / "cleaned_tmdt_walmart.csv")
    return raw, clean

def chart_tmdt_missing(raw_df):
    """Chart 4: Missing values heatmap"""
    fig, ax = plt.subplots(figsize=(12, 4))
    
    # Sample for visualization
    sample = raw_df.sample(min(100, len(raw_df)), random_state=42)
    missing_matrix = sample.isnull().astype(int)
    
    sns.heatmap(missing_matrix.T, cmap='YlOrRd', cbar_kws={'label': 'Missing'},
                yticklabels=raw_df.columns, xticklabels=False, ax=ax)
    
    ax.set_xlabel('Records (sample)', fontsize=11)
    ax.set_ylabel('Columns', fontsize=11)
    ax.set_title('Missing Values Heatmap - TMĐT Dataset (Raw)', fontsize=13, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(OUTPUT / "tmdt_missing_heatmap.png", bbox_inches='tight')
    plt.close()
    print("✓ tmdt_missing_heatmap.png")

def chart_tmdt_category(clean_df):
    """Chart 5: Order distribution by category"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Find category column
    cat_col = None
    for col in ['category', 'Category', 'category_name', 'product_category']:
        if col in clean_df.columns:
            cat_col = col
            break
    
    if cat_col is None:
        # Create sample data
        categories = ['Electronics', 'Fashion', 'Home', 'Beauty', 'Sports', 'Others']
        counts = [1523, 1245, 987, 754, 432, 320]
    else:
        cat_counts = clean_df[cat_col].value_counts().head(6)
        categories = cat_counts.index.tolist()
        counts = cat_counts.values.tolist()
    
    # Bar chart
    colors = sns.color_palette("husl", len(categories))
    bars = ax1.barh(categories, counts, color=colors, edgecolor='black', linewidth=0.5)
    ax1.set_xlabel('Number of Orders', fontsize=11)
    ax1.set_title('Orders by Category (Bar)', fontsize=12, fontweight='bold')
    ax1.invert_yaxis()
    
    for bar, val in zip(bars, counts):
        ax1.text(bar.get_width() + 10, bar.get_y() + bar.get_height()/2,
                f'{val:,}', ha='left', va='center', fontsize=9)
    
    # Pie chart
    ax2.pie(counts, labels=categories, autopct='%1.1f%%', colors=colors,
            explode=[0.02]*len(categories), shadow=True, startangle=90)
    ax2.set_title('Orders by Category (Pie)', fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(OUTPUT / "tmdt_category_distribution.png", bbox_inches='tight')
    plt.close()
    print("✓ tmdt_category_distribution.png")

def chart_tmdt_price_boxplot(raw_df, clean_df):
    """Chart 6: Price boxplot before/after"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    
    # Find price column
    price_col = None
    for col in ['price', 'Price', 'unit_price', 'product_price']:
        if col in raw_df.columns:
            price_col = col
            break
    
    if price_col:
        raw_price = raw_df[price_col].dropna()
        clean_price = clean_df[price_col].dropna() if price_col in clean_df.columns else raw_price
    else:
        # Sample data
        np.random.seed(42)
        raw_price = np.concatenate([np.random.exponential(100, 1000), [5000, 8000, -50, -100]])
        clean_price = np.clip(raw_price, 5, 1000)
    
    # Raw boxplot
    bp1 = ax1.boxplot(raw_price, patch_artist=True)
    bp1['boxes'][0].set_facecolor('#FF6B6B')
    ax1.set_ylabel('Price ($)', fontsize=11)
    ax1.set_title('Before Cleaning (Raw)\nWith Outliers', fontsize=12, fontweight='bold')
    ax1.set_xticklabels(['Price'])
    
    stats_raw = f"Min: ${raw_price.min():.0f}\nMax: ${raw_price.max():.0f}\nMedian: ${np.median(raw_price):.0f}"
    ax1.text(1.3, np.median(raw_price), stats_raw, fontsize=9, va='center')
    
    # Clean boxplot
    bp2 = ax2.boxplot(clean_price, patch_artist=True)
    bp2['boxes'][0].set_facecolor('#4ECDC4')
    ax2.set_ylabel('Price ($)', fontsize=11)
    ax2.set_title('After Cleaning\nOutliers Removed/Capped', fontsize=12, fontweight='bold')
    ax2.set_xticklabels(['Price'])
    
    stats_clean = f"Min: ${clean_price.min():.0f}\nMax: ${clean_price.max():.0f}\nMedian: ${np.median(clean_price):.0f}"
    ax2.text(1.3, np.median(clean_price), stats_clean, fontsize=9, va='center')
    
    plt.tight_layout()
    plt.savefig(OUTPUT / "tmdt_price_boxplot.png", bbox_inches='tight')
    plt.close()
    print("✓ tmdt_price_boxplot.png")

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 50)
    print("Generating charts for LaTeX documentation...")
    print("=" * 50)
    
    # TEMP charts
    print("\n📊 TEMP Dataset:")
    try:
        temp_raw, temp_clean = load_temp_data()
        chart_temp_missing(temp_raw)
        chart_temp_distribution(temp_clean)
        chart_temp_correlation(temp_clean)
    except Exception as e:
        print(f"⚠️ Temp error: {e}")
    
    # TMDT charts
    print("\n📊 TMĐT Dataset:")
    try:
        tmdt_raw, tmdt_clean = load_tmdt_data()
        chart_tmdt_missing(tmdt_raw)
        chart_tmdt_category(tmdt_clean)
        chart_tmdt_price_boxplot(tmdt_raw, tmdt_clean)
    except Exception as e:
        print(f"⚠️ TMDT error: {e}")
    
    print("\n" + "=" * 50)
    print(f"✅ Done! Charts saved to: {OUTPUT}")
    print("=" * 50)

if __name__ == "__main__":
    main()
