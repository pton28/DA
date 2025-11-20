"""
run_etl.py
Main orchestrator for the ETL pipeline.
Coordinates: Extract → Transform → Load.
Uses extracting.py, transforming.py, loading.py.
"""

import os
import glob
import re
import logging
from datetime import datetime
import duckdb
import pandas as pd

from extracting import extract_csv
from transforming import transform_data
from loading import load_to_duckdb

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

SOURCE_DIR = '../data/Raw'
CLEAN_DIR = '../data/Clean'
DATABASE_PATH = '../staging/staging.db'
OVERWRITE_TABLES = False

# Mapping table → primary key for upsert
DEFAULT_PRIMARY_KEYS = {
    'cleaned_products_api': 'us_item_id',
    'marketing_data': None,      # No key → append mode
    'walmart_customers_purchases': None,
    'walmart_products': 'product_id'
}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Save cleaned CSV
# -----------------------------------------------------------------------------

def save_cleaned_data(df, original_filename):
    os.makedirs(CLEAN_DIR, exist_ok=True)
    clean_path = os.path.join(CLEAN_DIR, f"cleaned_{original_filename}")
    try:
        df.to_csv(clean_path, index=False, encoding='utf-8')
        logger.info(f"[SAVE] Cleaned file saved: {clean_path}")
    except Exception as e:
        logger.exception(f"[SAVE] Failed saving cleaned file {clean_path}: {e}")

# -----------------------------------------------------------------------------
# ETL Runner
# -----------------------------------------------------------------------------

def run_etl(source_dir=SOURCE_DIR):
    results = {
        'processed': [],
        'skipped': [],
        'errors': []
    }

    csv_files = glob.glob(os.path.join(source_dir, '*.csv'))
    if not csv_files:
        logger.warning('[RUN] No CSV files found in source directory.')
        return results

    with duckdb.connect(database=DATABASE_PATH) as conn:
        for file_path in csv_files:
            file_name = os.path.basename(file_path)
            table_name = re.sub(r"[^0-9a-zA-Z_]", "_", os.path.splitext(file_name)[0])
            if re.match(r"^[0-9]", table_name):
                table_name = 't_' + table_name

            logger.info(f"\n[RUN] Processing {file_name} → table: {table_name}")

            try:
                # Extract
                df = extract_csv(file_path)
                if df is None or df.empty:
                    logger.warning(f"[RUN] Empty dataframe from {file_name}, skipping.")
                    results['skipped'].append(file_name)
                    continue

                # Transform
                df = transform_data(df, table_name)

                # Save cleaned
                save_cleaned_data(df, file_name)

                # Load
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    logger.info(f"[LOAD] Dropped old table if existed: {table_name}")
                except Exception as e:
                    logger.warning(f"[LOAD] Failed dropping table {table_name}: {e}")
                    
                primary_key = DEFAULT_PRIMARY_KEYS.get(table_name)
                load_to_duckdb(
                    df,
                    table_name,
                    conn,
                    primary_key=primary_key,
                    overwrite=OVERWRITE_TABLES
                )

                results['processed'].append(file_name)

            except Exception as e:
                logger.exception(f"[RUN] Failed processing {file_name}: {e}")
                results['errors'].append({
                    'file': file_name,
                    'error': str(e)
                })
                continue

    logger.info(
        f"\n[RUN] Completed. Processed: {len(results['processed'])}, "
        f"Skipped: {len(results['skipped'])}, Errors: {len(results['errors'])}"
    )
    return results

# -----------------------------------------------------------------------------
# Main CLI
# -----------------------------------------------------------------------------

if __name__ == '__main__':
    start = datetime.now()
    logger.info("[MAIN] Starting ETL pipeline...")
    result = run_etl()
    end = datetime.now()

    logger.info(f"[MAIN] ETL finished in {(end - start).total_seconds():.2f}s")
    logger.info(f"[MAIN] Summary: {result}")
