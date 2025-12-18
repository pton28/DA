#!/usr/bin/env python
"""Debug script to trace date_key issue from raw → standardized → fact"""

import pandas as pd
from pathlib import Path

base = Path("d:/BK_Document/251/DoAn/New/DA")

print("=" * 80)
print("TRACING DATE_KEY ISSUE: Raw → Standardized → Fact")
print("=" * 80)

# Step 1: Raw Data
print("\n[STEP 1] RAW DATA - Temp.csv")
print("-" * 80)
raw = pd.read_csv(base / "data/Raw/Temp.csv")
print(f"Total rows: {len(raw):,}")
print(f"Date column - Non-null: {raw['Date'].notna().sum():,}, Null: {raw['Date'].isna().sum():,}")
print("\nFirst 10 rows:")
print(raw[['Store', 'Date', 'Weekly_Sales']].head(10).to_string())

# Step 2: Standardized Data
print("\n\n[STEP 2] STANDARDIZED DATA - std_store_performance.csv")
print("-" * 80)
std = pd.read_csv(base / "data/Golden/standardized/std_store_performance.csv")
print(f"Total rows: {len(std):,}")
print(f"sale_date column - Non-null: {std['sale_date'].notna().sum():,}, Null: {std['sale_date'].isna().sum():,}")
print("\nFirst 10 rows:")
print(std[['store_id', 'sale_date', 'weekly_sales']].head(10).to_string())

# Step 3: Check blank date rows in standardized
if std['sale_date'].isna().sum() > 0:
    print("\n[ISSUE] Found rows with BLANK sale_date in standardized:")
    blank_rows = std[std['sale_date'].isna()].head(5)
    print(blank_rows[['store_id', 'sale_date', 'weekly_sales']].to_string())

# Step 4: Fact Data
print("\n\n[STEP 3] FACT DATA - FACT_STORE_PERFORMANCE.csv")
print("-" * 80)
fact = pd.read_csv(base / "data/Golden/facts/FACT_STORE_PERFORMANCE.csv")
print(f"Total rows: {len(fact):,}")
print(f"date_key column - Valid (>0): {(fact['date_key'] > 0).sum():,}, Invalid (-1): {(fact['date_key'] == -1).sum():,}")
print("\nFirst 10 rows:")
print(fact[['performance_id', 'date_key', 'store_key', 'weekly_sales']].head(10).to_string())

# Step 4: Analysis
print("\n\n[ANALYSIS] Data Loss Summary")
print("-" * 80)
print(f"Raw Data rows:        {len(raw):,}")
print(f"Standardized rows:    {len(std):,}")
print(f"Fact rows (after fix): {len(fact):,}")
print(f"\nRows removed in Silver layer (Raw → Standardized): {len(raw) - len(std):,}")
print(f"Rows removed in Golden layer (Standardized → Fact): {len(std) - len(fact):,}")

# Root cause
print("\n[ROOT CAUSE]")
if std['sale_date'].isna().sum() > 0:
    print(f"✗ Standardized data HAS {std['sale_date'].isna().sum():,} rows with BLANK sale_date")
    print("  → These should have been removed in Silver layer!")
    print("  → Check: pipelines/silver/transforming.py - transform_temp() function")
else:
    print("✓ Standardized data has NO blank dates")
    print("→ Data quality is OK at standardized level")
