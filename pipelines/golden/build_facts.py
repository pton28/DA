"""
Build 3 independent FACT tables for 3 Star Schemas - GALAXY SCHEMA

Star Schema 1: FACT_SALES (Retail, 2024-2025)
Star Schema 2: FACT_STORE_PERFORMANCE (Store Performance, 2010-2012)
Star Schema 3: FACT_ECOMMERCE_SALES (E-commerce, 2019)
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
    """Create 3 independent fact tables for Galaxy Schema."""

    def __init__(self, standardized_dir: Path, dimensions_dir: Path, output_dir: Path):
        self.std_dir = Path(standardized_dir)
        self.dim_dir = Path(dimensions_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Load all sources
        self.df_purchases: Optional[pd.DataFrame] = None
        self.df_store_performance: Optional[pd.DataFrame] = None
        self.df_ecommerce_sales: Optional[pd.DataFrame] = None
        
        # Dimensions for each star schema
        self.dim_product: Optional[pd.DataFrame] = None
        self.dim_customer: Optional[pd.DataFrame] = None
        self.dim_payment: Optional[pd.DataFrame] = None
        self.dim_category: Optional[pd.DataFrame] = None
        
        self.dim_store: Optional[pd.DataFrame] = None
        self.dim_date_store: Optional[pd.DataFrame] = None
        self.dim_temperature: Optional[pd.DataFrame] = None
        
        self.dim_ecommerce_product: Optional[pd.DataFrame] = None
        self.dim_ecommerce_category: Optional[pd.DataFrame] = None
        self.dim_ecommerce_brand: Optional[pd.DataFrame] = None
        
        self._load_sources()

    def _load_sources(self) -> None:
        """Load standardized data and dimension tables."""
        # Load fact source data
        try:
            self.df_purchases = pd.read_csv(self.std_dir / "std_customer_purchases.csv")
            logger.info("Loaded std_customer_purchases.csv (%d rows)", len(self.df_purchases))
        except Exception as exc:
            logger.warning("Could not load std_customer_purchases.csv: %s", exc)

        try:
            self.df_store_performance = pd.read_csv(self.std_dir / "std_store_performance.csv")
            logger.info("Loaded std_store_performance.csv (%d rows)", len(self.df_store_performance))
        except Exception as exc:
            logger.warning("Could not load std_store_performance.csv: %s", exc)

        try:
            self.df_ecommerce_sales = pd.read_csv(self.std_dir / "std_ecommerce_sales.csv")
            logger.info("Loaded std_ecommerce_sales.csv (%d rows)", len(self.df_ecommerce_sales))
        except Exception as exc:
            logger.warning("Could not load std_ecommerce_sales.csv: %s", exc)

        # Load dimension tables
        def load_dim(filename: str) -> Optional[pd.DataFrame]:
            try:
                df = pd.read_csv(self.dim_dir / filename)
                logger.info("Loaded %s (%d rows)", filename, len(df))
                return df
            except Exception as exc:
                logger.warning("Could not load %s: %s", filename, exc)
                return None

        # Star Schema 1 dimensions
        self.dim_product = load_dim("DIM_PRODUCT.csv")
        self.dim_customer = load_dim("DIM_CUSTOMER.csv")
        self.dim_payment = load_dim("DIM_PAYMENT.csv")
        self.dim_category = load_dim("DIM_CATEGORY.csv")
        
        # Star Schema 2 dimensions
        self.dim_store = load_dim("DIM_STORE.csv")
        self.dim_date_store = load_dim("DIM_DATE_STORE.csv")
        self.dim_temperature = load_dim("DIM_TEMPERATURE.csv")
        
        # Star Schema 3 dimensions
        self.dim_ecommerce_product = load_dim("DIM_ECOMMERCE_PRODUCT.csv")
        self.dim_ecommerce_category = load_dim("DIM_ECOMMERCE_CATEGORY.csv")
        self.dim_ecommerce_brand = load_dim("DIM_ECOMMERCE_BRAND.csv")

    # ================================================================
    # STAR SCHEMA 1: FACT_SALES (Retail Sales 2024-2025)
    # ================================================================
    def build_fact_sales(self) -> Optional[pd.DataFrame]:
        """Build FACT_SALES for retail customer transaction analysis."""
        if self.df_purchases is None:
            logger.error("Purchases data missing; cannot build FACT_SALES")
            return None

        fact = self.df_purchases.copy()
        
        # Parse purchase_date to date_key
        fact["purchase_date"] = pd.to_datetime(fact["purchase_date"], errors="coerce")
        fact["date_key"] = pd.to_numeric(fact["purchase_date"].dt.strftime("%Y%m%d"), errors="coerce").fillna(-1).astype(int)

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

        # Create sale_id (surrogate key for fact)
        fact.insert(0, "sale_id", range(1, len(fact) + 1))

        # Convert flag columns to binary integers
        fact["discount_applied"] = fact["discount_applied_flag"].fillna(0).astype(int)
        fact["repeat_customer"] = fact["repeat_customer_flag"].fillna(0).astype(int)
        
        # Convert measures to proper types
        fact["purchase_amount"] = pd.to_numeric(fact["purchase_amount"], errors="coerce").fillna(0.0)
        fact["rating"] = pd.to_numeric(fact["rating"], errors="coerce").fillna(0.0)

        # Select final fact columns
        fact_final = fact[
            [
                "sale_id",
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
        ].copy()

        # Fill missing foreign keys with -1 (unknown dimension)
        fk_cols = ["customer_key", "product_key", "payment_key", "category_key"]
        for col in fk_cols:
            if col in fact_final.columns:
                fact_final[col] = fact_final[col].fillna(-1).astype(int)

        output_path = self.output_dir / "FACT_SALES.csv"
        fact_final.to_csv(output_path, index=False)
        logger.info("FACT_SALES built -> %s (%d rows)", output_path, len(fact_final))
        logger.info("  Total sales: $%.2f, Avg: $%.2f", 
                   fact_final["purchase_amount"].sum(), fact_final["purchase_amount"].mean())
        
        return fact_final

    # ================================================================
    # STAR SCHEMA 2: FACT_STORE_PERFORMANCE (Store Performance 2010-2012)
    # ================================================================
    def build_fact_store_performance(self) -> Optional[pd.DataFrame]:
        """Build FACT_STORE_PERFORMANCE for store weekly performance with weather."""
        if self.df_store_performance is None:
            logger.warning("Store performance data missing; cannot build FACT_STORE_PERFORMANCE")
            return None

        fact = self.df_store_performance.copy()
        initial_rows = len(fact)

        # Remove rows with missing sale_date (DATA QUALITY FIX)
        fact = fact.dropna(subset=["sale_date"])
        removed_rows = initial_rows - len(fact)
        if removed_rows > 0:
            logger.warning("Removed %d rows with missing sale_date (%.2f%% of data)", 
                          removed_rows, (removed_rows / initial_rows) * 100)

        # Parse sale_date to date_key
        fact["sale_date"] = pd.to_datetime(fact["sale_date"], errors="coerce")
        fact["date_key"] = pd.to_numeric(fact["sale_date"].dt.strftime("%Y%m%d"), errors="coerce").fillna(-1).astype(int)

        # Join store dimension
        if self.dim_store is not None:
            fact = fact.merge(
                self.dim_store[["store_id", "store_key"]],
                on="store_id",
                how="left",
            )
        
        # Classify temperature into categories
        def get_temp_category(temp):
            if pd.isna(temp):
                return -1
            if temp < 32:
                return 1  # Freezing
            elif temp < 50:
                return 2  # Cold
            elif temp < 70:
                return 3  # Cool
            elif temp < 85:
                return 4  # Warm
            else:
                return 5  # Hot
        
        fact["temp_category_key"] = fact["temperature"].apply(get_temp_category)

        # Create performance_id
        fact.insert(0, "performance_id", range(1, len(fact) + 1))

        # Select final columns
        fact_final = fact[
            [
                "performance_id",
                "date_key",
                "store_key",
                "temp_category_key",
                "weekly_sales",
                "temperature",
                "fuel_price",
                "cpi",
                "unemployment",
                "holiday_flag",
            ]
        ].copy()

        # Fill missing foreign keys
        fk_cols = ["store_key", "temp_category_key"]
        for col in fk_cols:
            if col in fact_final.columns:
                fact_final[col] = fact_final[col].fillna(-1).astype(int)
        
        # Fill missing measures
        fact_final["weekly_sales"] = fact_final["weekly_sales"].fillna(0.0)
        fact_final["temperature"] = fact_final["temperature"].fillna(0.0)
        fact_final["fuel_price"] = fact_final["fuel_price"].fillna(0.0)
        fact_final["cpi"] = fact_final["cpi"].fillna(0.0)
        fact_final["unemployment"] = fact_final["unemployment"].fillna(0.0)
        fact_final["holiday_flag"] = fact_final["holiday_flag"].fillna(0).astype(int)

        output_path = self.output_dir / "FACT_STORE_PERFORMANCE.csv"
        fact_final.to_csv(output_path, index=False)
        logger.info("FACT_STORE_PERFORMANCE built -> %s (%d rows)", output_path, len(fact_final))
        logger.info("  Total weekly sales: $%.2f, Avg temp: %.1f°F",
                   fact_final["weekly_sales"].sum(), fact_final["temperature"].mean())
        
        # Validation: Check date_key quality
        invalid_dates = len(fact_final[fact_final["date_key"] == -1])
        if invalid_dates > 0:
            logger.warning("⚠️ Found %d rows with date_key = -1 (no valid dates)", invalid_dates)
        else:
            logger.info("✓ All %d rows have valid dates", len(fact_final))
        
        return fact_final

    # ================================================================
    # STAR SCHEMA 3: FACT_ECOMMERCE_SALES (E-commerce 2019)
    # ================================================================
    def build_fact_ecommerce_sales(self) -> Optional[pd.DataFrame]:
        """Build FACT_ECOMMERCE_SALES for e-commerce product catalog analysis."""
        if self.df_ecommerce_sales is None:
            logger.warning("E-commerce data missing; cannot build FACT_ECOMMERCE_SALES")
            return None

        fact = self.df_ecommerce_sales.copy()

        # Join ecommerce product dimension (use product_id as key)
        if self.dim_ecommerce_product is not None:
            fact = fact.merge(
                self.dim_ecommerce_product[["product_id", "ecommerce_product_key"]],
                on="product_id",
                how="left",
            )
        
        # Join category dimension
        if self.dim_ecommerce_category is not None:
            fact = fact.merge(
                self.dim_ecommerce_category[["root_category", "sub_category", "ecommerce_category_key"]],
                on=["root_category", "sub_category"],
                how="left",
            )
        
        # Join brand dimension
        if self.dim_ecommerce_brand is not None:
            fact = fact.merge(
                self.dim_ecommerce_brand[["brand", "brand_key"]],
                on="brand",
                how="left",
            )

        # Create ecommerce_sale_id
        fact.insert(0, "ecommerce_sale_id", range(1, len(fact) + 1))

        # Convert measures
        fact["list_price"] = pd.to_numeric(fact["list_price"], errors="coerce").fillna(0.0)
        fact["sale_price"] = pd.to_numeric(fact["sale_price"], errors="coerce").fillna(0.0)
        fact["discount_amount"] = pd.to_numeric(fact["discount_amount"], errors="coerce").fillna(0.0)
        fact["discount_pct"] = pd.to_numeric(fact["discount_pct"], errors="coerce").fillna(0.0)
        
        # Convert available flag
        if "available" in fact.columns:
            fact["available_flag"] = fact["available"].apply(
                lambda x: 1 if str(x).lower() in ["true", "1", "yes"] else 0
            )
        else:
            fact["available_flag"] = 0

        # Select final columns
        fact_final = fact[
            [
                "ecommerce_sale_id",
                "ecommerce_product_key",
                "ecommerce_category_key",
                "brand_key",
                "list_price",
                "sale_price",
                "discount_amount",
                "discount_pct",
                "available_flag",
            ]
        ].copy()

        # Fill missing foreign keys
        fk_cols = ["ecommerce_product_key", "ecommerce_category_key", "brand_key"]
        for col in fk_cols:
            if col in fact_final.columns:
                fact_final[col] = fact_final[col].fillna(-1).astype(int)

        output_path = self.output_dir / "FACT_ECOMMERCE_SALES.csv"
        fact_final.to_csv(output_path, index=False)
        logger.info("FACT_ECOMMERCE_SALES built -> %s (%d rows)", output_path, len(fact_final))
        logger.info("  Avg list price: $%.2f, Avg discount: %.1f%%",
                   fact_final["list_price"].mean(), fact_final["discount_pct"].mean())
        
        return fact_final

    def build_all(self) -> Dict[str, Optional[pd.DataFrame]]:
        """Build all 3 fact tables for Galaxy Schema."""
        logger.info("=" * 80)
        logger.info("BUILDING 3 FACT TABLES - GALAXY SCHEMA")
        logger.info("=" * 80)

        facts: Dict[str, Optional[pd.DataFrame]] = {
            "sales": self.build_fact_sales(),
            "store_performance": self.build_fact_store_performance(),
            "ecommerce_sales": self.build_fact_ecommerce_sales(),
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
