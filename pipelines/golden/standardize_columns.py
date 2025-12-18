"""
Standardize column names and key fields for Golden layer inputs.

SIMPLIFIED: Chỉ dùng walmart_customer_purchases.csv cho FACT_SALES
(đã đủ: demographics, rating, revenue, payment, product info)

Outputs:
- data/Golden/standardized/std_customer_purchases.csv (Star Schema 1 - ONLY source)
- data/Golden/standardized/std_store_performance.csv (Star Schema 2)
- data/Golden/standardized/std_ecommerce_sales.csv (Star Schema 3)
- data/Golden/standardized/product_master.csv (từ purchases only)

REMOVED: api_products, walmart_products, marketing_data (không cần thiết)
"""

from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def normalize_product_name(name: str) -> str:
    """Normalize product names for deterministic hashing."""
    if not isinstance(name, str):
        return ""
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9]+", " ", name)
    return " ".join(name.split())


def build_product_ids(series: pd.Series, prefix: str = "PROD") -> pd.Series:
    """Create stable product ids from product names."""
    def _build(name: str) -> Optional[str]:
        norm = normalize_product_name(name)
        if not norm:
            return None
        digest = hashlib.md5(norm.encode("utf-8")).hexdigest()[:10]
        return f"{prefix}_{digest}"

    return series.apply(_build)


def first_not_null(values: pd.Series):
    """Return the first non-null value or None."""
    values = values.dropna()
    return values.iloc[0] if not values.empty else None


class ColumnStandardizer:
    """Standardize columns coming from the cleaned layer."""

    def __init__(self, clean_dir: Path, output_dir: Path):
        self.clean_dir = Path(clean_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # api_df REMOVED - không cần thiết
        # marketing_df REMOVED - không link với fact tables
        # walmart_df REMOVED - không cần thiết
        self.purchases_df: Optional[pd.DataFrame] = None  # CHỈ DUY NHẤT source
        self.temp_df: Optional[pd.DataFrame] = None
        self.tmdt_walmart_df: Optional[pd.DataFrame] = None

    # ------------------------------------------------------------------ #
    # Standardization helpers per dataset
    # ------------------------------------------------------------------ #
    
    # REMOVED: standardize_api_products() - không cần thiết cho FACT_SALES
    # REMOVED: standardize_walmart_products() - không cần thiết cho FACT_SALES
    # CHỈ GIỮ: standardize_customer_purchases() - đủ data cho Star Schema 1
    
    def _standardize_api_products_UNUSED(self) -> Optional[pd.DataFrame]:
        path = self.clean_dir / "cleaned_cleaned_products_API.csv"
        if not path.exists():
            logger.warning("API products file not found: %s", path)
            return None

        df = pd.read_csv(path)
        df = df.rename(
            columns={
                "title": "product_name",
                "reviews": "review_count",
                "seller_name": "brand",
            }
        )

        df["source_product_id"] = df["product_id"]
        df["product_id"] = build_product_ids(df["product_name"])
        df["source"] = "api"

        keep_cols = [
            "product_id",
            "product_name",
            "brand",
            "rating",
            "review_count",
            "seller_id",
            "two_day_shipping",
            "free_shipping",
            "free_shipping_with_walmart_plus",
            "out_of_stock",
            "sponsored",
            "muliple_options_available",
            "price_per_unit",
            "fetch_time",
            "source_product_id",
            "source",
        ]
        df = df[[c for c in keep_cols if c in df.columns]]

        output_path = self.output_dir / "std_api_products.csv"
        df.to_csv(output_path, index=False)
        logger.info("API products standardized -> %s (%d rows)", output_path, len(df))
        return df

    # REMOVED: standardize_marketing_data()
    # Lý do: File marketing_data không link với FACT_SALES/DIM_PRODUCT
    # Gây vấn đề cross-filter trong Power BI → Dashboard bị xám/rỗng
    # File tmdt_walmart.csv đã đủ chi tiết cho e-commerce analysis

    def _standardize_walmart_products_UNUSED(self) -> Optional[pd.DataFrame]:
        path = self.clean_dir / "cleaned_walmart_products.csv"
        if not path.exists():
            logger.warning("Walmart products file not found: %s", path)
            return None

        df = pd.read_csv(path)
        df = df.rename(columns={"final_price": "price"})

        df["source_product_id"] = df["product_id"]
        df["product_id"] = build_product_ids(df["product_name"])
        df["source"] = "walmart"

        output_path = self.output_dir / "std_walmart_products.csv"
        df.to_csv(output_path, index=False)
        logger.info("Walmart products standardized -> %s (%d rows)", output_path, len(df))
        return df

    def standardize_customer_purchases(self) -> Optional[pd.DataFrame]:
        path = self.clean_dir / "cleaned_Walmart_customer_purchases.csv"
        if not path.exists():
            logger.warning("Customer purchases file not found: %s", path)
            return None

        df = pd.read_csv(path)
        df["product_id"] = build_product_ids(df["product_name"])
        df["purchase_date"] = pd.to_datetime(
            df["purchase_date"], format="%m-%d-%y", errors="coerce"
        )
        df["purchase_amount"] = pd.to_numeric(df["purchase_amount"], errors="coerce").astype(float)
        df["discount_applied_flag"] = (
            df["discount_applied"].fillna("").astype(str).str.lower().eq("yes")
        ).astype(int)  # store as 0/1 in CSV
        df["repeat_customer_flag"] = (
            df["repeat_customer"].fillna("").astype(str).str.lower().eq("yes")
        ).astype(int)  # store as 0/1 in CSV
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce").astype(float)
        df["source"] = "purchases"

        output_path = self.output_dir / "std_customer_purchases.csv"
        df.to_csv(output_path, index=False)
        logger.info(
            "Customer purchases standardized -> %s (%d rows)", output_path, len(df)
        )
        return df

    def standardize_store_performance(self) -> Optional[pd.DataFrame]:
        """Standardize store performance data for FACT_STORE_PERFORMANCE star schema"""
        path = self.clean_dir / "cleaned_Temp.csv"
        if not path.exists():
            logger.warning("Temp data file not found: %s", path)
            return None

        df = pd.read_csv(path)
        
        # Rename columns for consistency
        rename_map = {
            "Store": "store_id",
            "store": "store_id",
            "Date": "sale_date",
            "date": "sale_date",
            "Weekly_Sales": "weekly_sales",
            "Temperature": "temperature",
            "Fuel_Price": "fuel_price",
            "CPI": "cpi",
            "Unemployment": "unemployment",
            "Holiday_Flag": "holiday_flag"
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        
        # Parse sale_date - Format is DD-MM-YYYY (Day-Month-Year)
        # Examples: 05-02-2010 (5 Feb 2010), 19-02-2010 (19 Feb 2010)
        df["sale_date"] = pd.to_datetime(df["sale_date"], format="%d-%m-%Y", errors="coerce")
        
        # Convert numeric columns
        numeric_cols = ["store_id", "weekly_sales", "temperature", "fuel_price", "cpi", "unemployment"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Convert holiday flag to binary
        if "holiday_flag" in df.columns:
            df["holiday_flag"] = df["holiday_flag"].fillna(0).astype(int)
        
        df["source"] = "store_performance"
        
        output_path = self.output_dir / "std_store_performance.csv"
        df.to_csv(output_path, index=False)
        logger.info("Store performance standardized -> %s (%d rows)", output_path, len(df))
        return df

    def standardize_ecommerce_sales(self) -> Optional[pd.DataFrame]:
        """Standardize e-commerce data for FACT_ECOMMERCE_SALES star schema (NO crawl_timestamp)"""
        path = self.clean_dir / "cleaned_tmdt_walmart.csv"
        if not path.exists():
            logger.warning("TMDT Walmart file not found: %s", path)
            return None

        df = pd.read_csv(path)
        
        # Rename key columns (REMOVE crawl_timestamp entirely)
        df = df.rename(columns={
            "Uniq Id": "ecommerce_id",
            "Product Name": "product_name",
            "List Price": "list_price",
            "Sale Price": "sale_price",
            "Brand": "brand",
            "Available": "is_available"
        })
        
        # Drop crawl-related columns entirely (as requested)
        cols_to_drop = ["Crawl Timestamp", "crawl_timestamp", "crawl_day", "crawl_dayofweek"]
        for col in cols_to_drop:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        # Generate product_id from product_name
        df["product_id"] = build_product_ids(df["product_name"])
        
        # Parse category into root and subcategory (column is "category" not "category_name")
        if "category" in df.columns and "root_category" not in df.columns:
            category_split = df["category"].astype(str).str.split(" \\| ", n=1, expand=True)
            df["root_category"] = category_split[0] if 0 in category_split.columns else None
            df["sub_category"] = category_split[1] if 1 in category_split.columns else None
        
        # Convert numeric columns
        numeric_cols = ["list_price", "sale_price", "discount_percentage"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Calculate discount amount and percentage
        if "list_price" in df.columns and "sale_price" in df.columns:
            df["discount_amount"] = (df["list_price"] - df["sale_price"]).clip(lower=0)
            df["discount_pct"] = ((df["discount_amount"] / df["list_price"]) * 100).round(2)
        
        # Convert available to binary flag (for FACT measures)
        if "is_available" in df.columns:
            df["is_available"] = df["is_available"].apply(
                lambda x: 1 if str(x).lower() in ["true", "1", "yes"] else 0
            )
        
        df["source"] = "ecommerce"
        
        output_path = self.output_dir / "std_ecommerce_sales.csv"
        df.to_csv(output_path, index=False)
        logger.info("E-commerce sales standardized -> %s (%d rows, NO time dimension)", output_path, len(df))
        return df

    # ------------------------------------------------------------------ #
    # Product master builder
    # ------------------------------------------------------------------ #
    def build_product_master(self) -> None:
        frames: List[pd.DataFrame] = []

        def subset(df: pd.DataFrame, source: str) -> pd.DataFrame:
            df = df.copy()
            df["source"] = source
            columns = {
                "product_id": None,
                "product_name": None,
                "brand": None,
                "category_name": None,
                "root_category_name": None,
                "rating": None,
                "review_count": None,
                "source": None,
            }
            present_cols = [c for c in columns if c in df.columns]
            return df[present_cols]

        if self.purchases_df is not None:
            temp = self.purchases_df.rename(columns={"category": "category_name"})
            frames.append(subset(temp, "purchases"))

        # walmart_df REMOVED - không cần thiết
        # api_df REMOVED - không cần thiết
        # marketing_df REMOVED - không liên kết với fact tables

        if not frames:
            logger.warning("No product sources found, skipping product_master.csv")
            return

        combined = pd.concat(frames, ignore_index=True)

        # Chỉ aggregate columns có sẵn trong purchases data
        agg_funcs: Dict[str, callable] = {
            "product_name": first_not_null,
            "category_name": first_not_null,
            "rating": first_not_null,
            "source": lambda s: ",".join(sorted(set(s.dropna()))),
        }
        # Add optional columns if exist
        if "brand" in combined.columns:
            agg_funcs["brand"] = first_not_null
        if "review_count" in combined.columns:
            agg_funcs["review_count"] = first_not_null
        if "root_category_name" in combined.columns:
            agg_funcs["root_category_name"] = first_not_null
            
        master = (
            combined.groupby("product_id", dropna=True)
            .agg(agg_funcs)
            .reset_index()
            .dropna(subset=["product_id"])
        )

        output_path = self.output_dir / "product_master.csv"
        master.to_csv(output_path, index=False)
        logger.info("Product master created -> %s (%d products)", output_path, len(master))

    # ------------------------------------------------------------------ #
    def run(self) -> None:
        logger.info("=" * 80)
        logger.info("COLUMN STANDARDIZATION - GOLDEN LAYER")
        logger.info("=" * 80)

        # Star Schema 1: CHỈ dùng customer_purchases (đủ data)
        self.purchases_df = self.standardize_customer_purchases()
        # api_df REMOVED - không cần
        # walmart_df REMOVED - không cần
        # marketing_df REMOVED - không link với fact tables
        
        # Star Schema 2 & 3 data sources
        self.standardize_store_performance()
        self.standardize_ecommerce_sales()

        self.build_product_master()


def main():
    base_dir = Path(__file__).resolve().parents[2]
    clean_dir = base_dir / "data" / "Clean"
    output_dir = base_dir / "data" / "Golden" / "standardized"

    ColumnStandardizer(clean_dir, output_dir).run()


if __name__ == "__main__":
    main()
