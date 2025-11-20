"""
Build dimension tables for the Golden layer star schema.

Dimensions produced:
- DIM_PRODUCT
- DIM_CUSTOMER
- DIM_DATE
- DIM_PAYMENT
- DIM_CATEGORY
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class DimensionBuilder:
    """Create all dimension tables for the star schema."""

    def __init__(self, standardized_dir: Path, output_dir: Path):
        self.std_dir = Path(standardized_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.df_products: Optional[pd.DataFrame] = None
        self.df_purchases: Optional[pd.DataFrame] = None
        self.df_walmart: Optional[pd.DataFrame] = None
        self._load_sources()

    def _load_sources(self) -> None:
        """Load standardized inputs."""
        try:
            self.df_products = pd.read_csv(self.std_dir / "product_master.csv")
            logger.info("Loaded product_master.csv (%d rows)", len(self.df_products))
        except Exception as exc:
            logger.warning("Could not load product_master.csv: %s", exc)

        try:
            self.df_purchases = pd.read_csv(self.std_dir / "std_customer_purchases.csv")
            logger.info("Loaded std_customer_purchases.csv (%d rows)", len(self.df_purchases))
        except Exception as exc:
            logger.warning("Could not load std_customer_purchases.csv: %s", exc)

        try:
            self.df_walmart = pd.read_csv(self.std_dir / "std_walmart_products.csv")
            logger.info("Loaded std_walmart_products.csv (%d rows)", len(self.df_walmart))
        except Exception as exc:
            logger.warning("Could not load std_walmart_products.csv: %s", exc)

    # ------------------------------------------------------------------ #
    def build_dim_product(self) -> Optional[pd.DataFrame]:
        if self.df_products is None:
            logger.error("product_master.csv is required to build DIM_PRODUCT")
            return None

        dim_product = self.df_products.copy()
        dim_product.insert(0, "product_key", range(1, len(dim_product) + 1))

        # Ensure consistent column order
        wanted = [
            "product_key",
            "product_id",
            "product_name",
            "brand",
            "category_name",
            "root_category_name",
            "rating",
            "review_count",
            "source",
        ]
        dim_product = dim_product.reindex(columns=wanted)

        output_path = self.output_dir / "DIM_PRODUCT.csv"
        dim_product.to_csv(output_path, index=False)
        logger.info("DIM_PRODUCT built -> %s (%d rows)", output_path, len(dim_product))
        return dim_product

    def build_dim_customer(self) -> Optional[pd.DataFrame]:
        if self.df_purchases is None:
            logger.error("std_customer_purchases.csv is required to build DIM_CUSTOMER")
            return None

        cols = ["customer_id", "age", "gender", "city"]
        dim_customer = self.df_purchases[cols].drop_duplicates(subset=["customer_id"]).copy()
        dim_customer.insert(0, "customer_key", range(1, len(dim_customer) + 1))

        dim_customer["age_group"] = pd.cut(
            dim_customer["age"],
            bins=[0, 18, 30, 45, 60, 120],
            labels=["<18", "18-30", "31-45", "46-60", "60+"],
        )

        output_path = self.output_dir / "DIM_CUSTOMER.csv"
        dim_customer.to_csv(output_path, index=False)
        logger.info("DIM_CUSTOMER built -> %s (%d rows)", output_path, len(dim_customer))
        return dim_customer

    def build_dim_date(self) -> pd.DataFrame:
        # Use purchase date range when available; otherwise default 2020-2030.
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2030, 12, 31)

        if self.df_purchases is not None and "purchase_date" in self.df_purchases.columns:
            dates = pd.to_datetime(self.df_purchases["purchase_date"], errors="coerce").dropna()
            if not dates.empty:
                start_date = dates.min().date()
                end_date = dates.max().date()
                # Extend a little for future reporting
                end_date = end_date + timedelta(days=90)
                start_date = start_date - timedelta(days=30)
                start_date = datetime.combine(start_date, datetime.min.time())
                end_date = datetime.combine(end_date, datetime.min.time())

        dates = pd.date_range(start=start_date, end=end_date, freq="D")
        dim_date = pd.DataFrame(
            {
                "date_key": dates.strftime("%Y%m%d").astype(int),
                "full_date": dates.date,
                "day": dates.day,
                "day_name": dates.strftime("%A"),
                "day_of_week": dates.dayofweek + 1,
                "is_weekend": (dates.dayofweek >= 5).astype(int),
                "month": dates.month,
                "month_name": dates.strftime("%B"),
                "quarter": dates.quarter,
                "week_of_year": dates.isocalendar().week.astype(int),
                "year": dates.year,
            }
        )

        output_path = self.output_dir / "DIM_DATE.csv"
        dim_date.to_csv(output_path, index=False)
        logger.info("DIM_DATE built -> %s (%d rows)", output_path, len(dim_date))
        return dim_date

    def build_dim_payment(self) -> Optional[pd.DataFrame]:
        if self.df_purchases is None:
            logger.error("std_customer_purchases.csv is required to build DIM_PAYMENT")
            return None

        methods = self.df_purchases["payment_method"].dropna().unique()
        dim_payment = pd.DataFrame(
            {
                "payment_key": range(1, len(methods) + 1),
                "payment_method": methods,
            }
        )

        output_path = self.output_dir / "DIM_PAYMENT.csv"
        dim_payment.to_csv(output_path, index=False)
        logger.info("DIM_PAYMENT built -> %s (%d rows)", output_path, len(dim_payment))
        return dim_payment

    def build_dim_category(self) -> Optional[pd.DataFrame]:
        frames = []

        if self.df_walmart is not None:
            temp = self.df_walmart[["category_name", "root_category_name"]].drop_duplicates()
            frames.append(temp)

        if self.df_purchases is not None:
            temp = (
                self.df_purchases[["category"]]
                .drop_duplicates()
                .rename(columns={"category": "category_name"})
            )
            temp["root_category_name"] = None
            frames.append(temp)

        if not frames:
            logger.error("No category sources found to build DIM_CATEGORY")
            return None

        dim_category = pd.concat(frames, ignore_index=True)
        dim_category = dim_category.drop_duplicates(subset=["category_name"])
        dim_category.insert(0, "category_key", range(1, len(dim_category) + 1))

        output_path = self.output_dir / "DIM_CATEGORY.csv"
        dim_category.to_csv(output_path, index=False)
        logger.info("DIM_CATEGORY built -> %s (%d rows)", output_path, len(dim_category))
        return dim_category

    def build_all(self) -> Dict[str, Optional[pd.DataFrame]]:
        logger.info("=" * 80)
        logger.info("BUILDING DIMENSIONS")
        logger.info("=" * 80)

        dims: Dict[str, Optional[pd.DataFrame]] = {
            "product": self.build_dim_product(),
            "customer": self.build_dim_customer(),
            "date": self.build_dim_date(),
            "payment": self.build_dim_payment(),
            "category": self.build_dim_category(),
        }

        logger.info("=" * 80)
        logger.info("DIMENSIONS COMPLETE")
        logger.info("=" * 80)
        return dims


def main():
    base_dir = Path(__file__).resolve().parents[2]
    std_dir = base_dir / "data" / "Golden" / "standardized"
    output_dir = base_dir / "data" / "Golden" / "dimensions"

    DimensionBuilder(std_dir, output_dir).build_all()


if __name__ == "__main__":
    main()
