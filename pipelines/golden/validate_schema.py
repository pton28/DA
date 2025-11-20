"""
Validate Golden layer star schema outputs.
Checks:
- File existence
- Primary key uniqueness
- Foreign key integrity for FACT_SALES
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[2]
DIM_DIR = BASE_DIR / "data" / "Golden" / "dimensions"
FACT_DIR = BASE_DIR / "data" / "Golden" / "facts"


def check_exists(path: Path) -> bool:
    if path.exists():
        logger.info("[OK] %s", path.name)
        return True
    logger.error("[MISSING] %s", path.name)
    return False


def check_primary_key(df: pd.DataFrame, pk: str, table: str) -> bool:
    if pk not in df.columns:
        logger.error("%s: primary key %s not found", table, pk)
        return False
    dup = df[pk].duplicated().sum()
    nulls = df[pk].isna().sum()
    if dup or nulls:
        logger.error("%s: pk duplicates=%d, nulls=%d", table, dup, nulls)
        return False
    logger.info("%s: primary key is valid (%d rows)", table, len(df))
    return True


def check_foreign_key(
    fact: pd.DataFrame,
    dim: pd.DataFrame,
    fk: str,
    pk: str,
    fact_name: str,
    dim_name: str,
) -> bool:
    if fk not in fact.columns:
        logger.error("%s: foreign key %s not found", fact_name, fk)
        return False
    missing = fact[fk].isna().sum()
    non_match = (~fact[fk].dropna().isin(dim[pk])).sum()
    if missing:
        logger.warning("%s: %d nulls in %s", fact_name, missing, fk)
    if non_match:
        logger.error("%s: %d orphaned keys in %s", fact_name, non_match, fk)
        return False
    logger.info("%s -> %s: %s valid", fact_name, dim_name, fk)
    return True


def validate_dimensions() -> Dict[str, pd.DataFrame]:
    dims: Dict[str, pd.DataFrame] = {}
    files = {
        "DIM_PRODUCT": "product_key",
        "DIM_CUSTOMER": "customer_key",
        "DIM_DATE": "date_key",
        "DIM_PAYMENT": "payment_key",
        "DIM_CATEGORY": "category_key",
    }
    for filename, pk in files.items():
        path = DIM_DIR / f"{filename}.csv"
        if not check_exists(path):
            continue
        df = pd.read_csv(path)
        if check_primary_key(df, pk, filename):
            dims[filename] = df
    return dims


def validate_fact_sales(dims: Dict[str, pd.DataFrame]) -> bool:
    path = FACT_DIR / "FACT_SALES.csv"
    if not check_exists(path):
        return False
    fact = pd.read_csv(path)
    ok = check_primary_key(fact, "transaction_id", "FACT_SALES")

    ok &= check_foreign_key(fact, dims["DIM_DATE"], "date_key", "date_key", "FACT_SALES", "DIM_DATE")
    ok &= check_foreign_key(
        fact, dims["DIM_CUSTOMER"], "customer_key", "customer_key", "FACT_SALES", "DIM_CUSTOMER"
    )
    ok &= check_foreign_key(
        fact, dims["DIM_PRODUCT"], "product_key", "product_key", "FACT_SALES", "DIM_PRODUCT"
    )
    ok &= check_foreign_key(
        fact, dims["DIM_PAYMENT"], "payment_key", "payment_key", "FACT_SALES", "DIM_PAYMENT"
    )
    ok &= check_foreign_key(
        fact, dims["DIM_CATEGORY"], "category_key", "category_key", "FACT_SALES", "DIM_CATEGORY"
    )
    return ok


def main() -> bool:
    logger.info("=" * 80)
    logger.info("START VALIDATION")
    logger.info("=" * 80)

    dims = validate_dimensions()
    required_dims = {"DIM_PRODUCT", "DIM_CUSTOMER", "DIM_DATE", "DIM_PAYMENT", "DIM_CATEGORY"}
    if not required_dims.issubset(dims.keys()):
        logger.error("Missing required dimensions: %s", required_dims - set(dims.keys()))
        return False

    facts_ok = validate_fact_sales(dims)
    logger.info("=" * 80)
    if facts_ok:
        logger.info("Validation passed")
    else:
        logger.error("Validation failed")
    logger.info("=" * 80)
    return facts_ok


if __name__ == "__main__":
    success = main()
    raise SystemExit(0 if success else 1)
