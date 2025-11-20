"""
Build fact tables for the Golden layer star schema.

Facts produced:
- FACT_SALES
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class FactBuilder:
    """Create fact tables using standardized data and dimensions."""

    def __init__(self, standardized_dir: Path, dimensions_dir: Path, output_dir: Path):
        self.std_dir = Path(standardized_dir)
        self.dim_dir = Path(dimensions_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.df_purchases: Optional[pd.DataFrame] = None
        self.dim_product: Optional[pd.DataFrame] = None
        self.dim_customer: Optional[pd.DataFrame] = None
        self.dim_date: Optional[pd.DataFrame] = None
        self.dim_payment: Optional[pd.DataFrame] = None
        self.dim_category: Optional[pd.DataFrame] = None
        self._load_sources()

    def _load_sources(self) -> None:
        try:
            self.df_purchases = pd.read_csv(self.std_dir / "std_customer_purchases.csv")
            logger.info("Loaded std_customer_purchases.csv (%d rows)", len(self.df_purchases))
        except Exception as exc:
            logger.error("Could not load std_customer_purchases.csv: %s", exc)

        def load_dim(filename: str) -> Optional[pd.DataFrame]:
            try:
                df = pd.read_csv(self.dim_dir / filename)
                logger.info("Loaded %s (%d rows)", filename, len(df))
                return df
            except Exception as exc:
                logger.error("Could not load %s: %s", filename, exc)
                return None

        self.dim_product = load_dim("DIM_PRODUCT.csv")
        self.dim_customer = load_dim("DIM_CUSTOMER.csv")
        self.dim_date = load_dim("DIM_DATE.csv")
        self.dim_payment = load_dim("DIM_PAYMENT.csv")
        self.dim_category = load_dim("DIM_CATEGORY.csv")

    # ------------------------------------------------------------------ #
    def build_fact_sales(self) -> Optional[pd.DataFrame]:
        if self.df_purchases is None:
            logger.error("Purchases data missing; cannot build FACT_SALES")
            return None

        fact = self.df_purchases.copy()
        fact["purchase_date"] = pd.to_datetime(fact["purchase_date"], errors="coerce")
        fact["date_key"] = fact["purchase_date"].dt.strftime("%Y%m%d").astype(int)

        # Join dimension surrogate keys
        if self.dim_product is not None:
            fact = fact.merge(
                self.dim_product[["product_id", "product_key"]],
                on="product_id",
                how="left",
            )
        if self.dim_customer is not None:
            fact = fact.merge(
                self.dim_customer[["customer_id", "customer_key"]],
                on="customer_id",
                how="left",
            )
        if self.dim_payment is not None:
            fact = fact.merge(
                self.dim_payment[["payment_method", "payment_key"]],
                on="payment_method",
                how="left",
            )
        if self.dim_category is not None:
            fact = fact.merge(
                self.dim_category[["category_name", "category_key"]],
                left_on="category",
                right_on="category_name",
                how="left",
            )

        fact.insert(0, "transaction_id", range(1, len(fact) + 1))
        fact["discount_applied"] = fact["discount_applied_flag"].fillna(0).astype(int)
        fact["repeat_customer"] = fact["repeat_customer_flag"].fillna(0).astype(int)
        fact["purchase_amount"] = pd.to_numeric(fact["purchase_amount"], errors="coerce").fillna(0.0)
        fact["rating"] = pd.to_numeric(fact["rating"], errors="coerce").fillna(0.0)

        fact_sales = fact[
            [
                "transaction_id",
                "date_key",
                "customer_key",
                "product_key",
                "payment_key",
                "category_key",
                "purchase_amount",
                "discount_applied",
                "rating",
                "repeat_customer",
            ]
        ]

        output_path = self.output_dir / "FACT_SALES.csv"
        fact_sales.to_csv(output_path, index=False)
        logger.info("FACT_SALES built -> %s (%d rows)", output_path, len(fact_sales))
        return fact_sales

    def build_all(self) -> Dict[str, Optional[pd.DataFrame]]:
        logger.info("=" * 80)
        logger.info("BUILDING FACT TABLES")
        logger.info("=" * 80)

        facts: Dict[str, Optional[pd.DataFrame]] = {
            "sales": self.build_fact_sales(),
        }

        logger.info("=" * 80)
        logger.info("FACT TABLES COMPLETE")
        logger.info("=" * 80)
        return facts


def main():
    base_dir = Path(__file__).resolve().parents[2]
    std_dir = base_dir / "data" / "Golden" / "standardized"
    dim_dir = base_dir / "data" / "Golden" / "dimensions"
    output_dir = base_dir / "data" / "Golden" / "facts"

    FactBuilder(std_dir, dim_dir, output_dir).build_all()


if __name__ == "__main__":
    main()
