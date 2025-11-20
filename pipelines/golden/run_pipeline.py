"""
Run the Golden layer pipeline end-to-end.

Steps:
1) Standardize columns from data/Clean into data/Golden/standardized
2) Build dimensions into data/Golden/dimensions
3) Build facts into data/Golden/facts
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import duckdb

from build_dims import DimensionBuilder
from build_facts import FactBuilder
from standardize_columns import ColumnStandardizer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    base_dir = Path(__file__).resolve().parents[2]
    clean_dir = base_dir / "data" / "Clean"
    std_dir = base_dir / "data" / "Golden" / "standardized"
    dim_dir = base_dir / "data" / "Golden" / "dimensions"
    fact_dir = base_dir / "data" / "Golden" / "facts"
    db_path = base_dir / "database" / "walmart_analytics.db"

    logger.info("=" * 80)
    logger.info("START GOLDEN PIPELINE")
    logger.info("=" * 80)

    ColumnStandardizer(clean_dir, std_dir).run()
    dims = DimensionBuilder(std_dir, dim_dir).build_all()
    facts = FactBuilder(std_dir, dim_dir, fact_dir).build_all()

    # Load all golden CSVs into DuckDB warehouse
    dim_files = sorted(dim_dir.glob("*.csv"))
    fact_files = sorted(fact_dir.glob("*.csv"))
    if not db_path.parent.exists():
        db_path.parent.mkdir(parents=True, exist_ok=True)

    def to_table_name(path: Path) -> str:
        stem = path.stem
        return re.sub(r"[^0-9a-zA-Z_]", "_", stem).lower()

    with duckdb.connect(database=str(db_path)) as conn:
        for csv_path in dim_files + fact_files:
            table_name = to_table_name(csv_path)
            conn.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto(?, header=True)",
                [str(csv_path)],
            )
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info("Loaded %s into DuckDB table %s (%d rows)", csv_path.name, table_name, row_count)

    logger.info("=" * 80)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 80)
    logger.info("Dimensions: %s", {k: len(v) if v is not None else 0 for k, v in dims.items()})
    logger.info("Facts: %s", {k: len(v) if v is not None else 0 for k, v in facts.items()})
    logger.info("Outputs stored under %s/data/Golden", base_dir)
    logger.info("DuckDB database updated at %s", db_path)
    logger.info("Golden pipeline finished successfully.")


if __name__ == "__main__":
    main()
