"""
extracting.py
Module for Extract stage of ETL.
Handles safe CSV reading, encoding detection, and optional DuckDB CSV reader.
"""


import os
import re
import csv
import logging
import duckdb
import pandas as pd
from charset_normalizer import from_path

logger = logging.getLogger(__name__)
USE_DUCKDB_READ = False

# -----------------------------
# Encoding Detection
# -----------------------------

def detect_encoding(file_path):
    try:
        detection = from_path(file_path, cp_isolation=None)
        best = detection.best()
        if best:
            return best.encoding, best.confidence
    except Exception as e:
        logger.debug(f"Encoding detection failed for {file_path}: {e}")
    return None, 0.0

# -----------------------------
# Robust CSV Reader
# -----------------------------

def safe_read_csv(file_path, **kwargs):
    """Try multiple encodings and fall back to latin1.
    
    Special handling for marketing_data.csv which has misaligned header/data
    (28 columns in header, 29 in data rows)
    """

    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    # Special case: marketing_data.csv needs manual CSV parsing to fix structure
    if 'marketing_data.csv' in file_path:
        logger.info(f"[EXTRACT] Detected marketing_data.csv - using manual CSV fix")
        return _read_marketing_data_with_fix(file_path)

    detected_encoding, confidence = detect_encoding(file_path)

    encodings_to_try = [
    detected_encoding,
    'utf-8', 'utf-8-sig',
    'cp1252', 'latin1', 'iso-8859-1',
    'gbk', 'big5', 'shift-jis'
    ]
    encodings_to_try = [e for e in dict.fromkeys([e for e in encodings_to_try if e])]

    for enc in encodings_to_try:
        try:
            df = pd.read_csv(file_path, encoding=enc, low_memory=False, **kwargs)
            logger.info(f"[EXTRACT] Read {file_path} with encoding={enc} (conf={confidence:.2f})")
            return df
        except Exception:
            continue

    logger.warning(f"[EXTRACT] Falling back to latin1 for {file_path}")
    df = pd.read_csv(file_path, encoding='latin1', low_memory=False, **kwargs)

    # Try repairing unicode
    for col in df.select_dtypes(include=['object']).columns:
        try:
            df[col] = df[col].astype(str).apply(lambda s: s.encode('latin1').decode('utf-8', errors='replace'))
        except Exception:
            pass

    return df


def _read_marketing_data_with_fix(file_path):
    """
    Fix marketing_data.csv structure: header has 28 cols, data rows have 29.
    Solution: Read with csv module, trim last column from each row, then create DataFrame.
    """
    rows = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            for row in reader:
                # Fix: Data có 29 cột, header có 28 cột → bỏ cột cuối
                if len(row) > len(header):
                    row = row[:len(header)]
                elif len(row) < len(header):
                    row = row + ['NA'] * (len(header) - len(row))
                
                rows.append(row)
        
        df = pd.DataFrame(rows, columns=header)
        logger.info(f"[EXTRACT] Fixed marketing_data structure: {len(df):,} rows × {len(df.columns)} cols")
        return df
        
    except UnicodeDecodeError:
        # Try other encodings if UTF-8 fails
        for enc in ['utf-8-sig', 'cp1252', 'latin1']:
            try:
                with open(file_path, 'r', encoding=enc) as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    rows = []
                    
                    for row in reader:
                        if len(row) > len(header):
                            row = row[:len(header)]
                        elif len(row) < len(header):
                            row = row + ['NA'] * (len(header) - len(row))
                        rows.append(row)
                    
                    df = pd.DataFrame(rows, columns=header)
                    logger.info(f"[EXTRACT] Fixed marketing_data with {enc}: {len(df):,} rows × {len(df.columns)} cols")
                    return df
            except Exception:
                continue
        
        raise RuntimeError(f"Cannot read {file_path} with any encoding")

# -----------------------------
# Extract Entry Point
# -----------------------------

def extract_csv(file_path):
    """Extract CSV using DuckDB read_csv_auto (optional) or safe pandas reader."""

    if USE_DUCKDB_READ:
        try:
            con = duckdb.connect(database=':memory:')
            df = con.execute(
            f"SELECT * FROM read_csv_auto('{file_path.replace('\\','/')}')").fetchdf()
            con.close()
            logger.info(f"[EXTRACT] DuckDB read_csv_auto succeeded for {file_path}")
            return df
        except Exception:
            logger.warning(f"[EXTRACT] DuckDB read_csv_auto failed for {file_path}, fallback to pandas")

    return safe_read_csv(file_path)