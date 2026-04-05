from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .config import BRONZE_DIR, GOLD_DIR, RAW_DATA_DIR, SILVER_DIR, ensure_local_directories


CRITICAL_SALES_COLUMNS = ["sale_id", "product_id", "customer_id", "sales_amount"]


@dataclass(slots=True)
class TransformArtifacts:
    bronze_files: list[Path]
    silver_files: list[Path]
    gold_files: list[Path]


def _load_latest_csv(dataset: str) -> pd.DataFrame:
    dataset_dir = RAW_DATA_DIR / dataset
    latest_file = sorted(dataset_dir.glob("*.csv"))[-1]
    dataframe = pd.read_csv(latest_file)
    dataframe["source_file"] = latest_file.name
    return dataframe


def _write_parquet(dataframe: pd.DataFrame, target_path: Path) -> Path:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(target_path, index=False)
    return target_path


def _normalize_products(products: pd.DataFrame) -> pd.DataFrame:
    normalized = products.copy()
    normalized["product_name"] = normalized["product_name"].str.strip()
    normalized["category"] = normalized["category"].str.strip().str.title()
    normalized["unit_price"] = normalized["unit_price"].astype(float)
    normalized["last_updated"] = pd.to_datetime(normalized["last_updated"], utc=True)
    return normalized


def _normalize_customers(customers: pd.DataFrame) -> pd.DataFrame:
    normalized = customers.copy()
    normalized["email"] = normalized["email"].str.strip().str.lower()
    normalized["loyalty_tier"] = normalized["loyalty_tier"].str.title()
    normalized["created_at"] = pd.to_datetime(normalized["created_at"], utc=True)
    return normalized


def _normalize_sales(sales: pd.DataFrame) -> pd.DataFrame:
    normalized = sales.copy()
    normalized["sale_date"] = pd.to_datetime(normalized["sale_date"], utc=True)
    normalized["quantity"] = normalized["quantity"].fillna(0).astype(int)
    normalized["unit_price"] = normalized["unit_price"].astype(float)
    normalized["sales_amount"] = normalized["sales_amount"].astype(float)
    normalized["payment_method"] = normalized["payment_method"].str.strip().str.lower()
    normalized = normalized.dropna(subset=CRITICAL_SALES_COLUMNS)
    return normalized


def _normalize_inventory(inventory: pd.DataFrame) -> pd.DataFrame:
    normalized = inventory.copy()
    normalized["on_hand_quantity"] = normalized["on_hand_quantity"].fillna(0).astype(int)
    normalized["reorder_level"] = normalized["reorder_level"].fillna(0).astype(int)
    normalized["stock_status"] = normalized["stock_status"].str.strip().str.lower()
    normalized["updated_at"] = pd.to_datetime(normalized["updated_at"], utc=True)
    return normalized


def build_medallion_layers() -> TransformArtifacts:
    ensure_local_directories()

    products_raw = _load_latest_csv("products")
    customers_raw = _load_latest_csv("customers")
    sales_raw = _load_latest_csv("sales")
    inventory_raw = _load_latest_csv("inventory")

    bronze_files = [
        _write_parquet(products_raw, BRONZE_DIR / "products.parquet"),
        _write_parquet(customers_raw, BRONZE_DIR / "customers.parquet"),
        _write_parquet(sales_raw, BRONZE_DIR / "sales.parquet"),
        _write_parquet(inventory_raw, BRONZE_DIR / "inventory.parquet"),
    ]

    products = _normalize_products(products_raw)
    customers = _normalize_customers(customers_raw)
    sales = _normalize_sales(sales_raw)
    inventory = _normalize_inventory(inventory_raw)

    silver_files = [
        _write_parquet(products, SILVER_DIR / "products.parquet"),
        _write_parquet(customers, SILVER_DIR / "customers.parquet"),
        _write_parquet(sales, SILVER_DIR / "sales.parquet"),
        _write_parquet(inventory, SILVER_DIR / "inventory.parquet"),
    ]

    daily_sales = (
        sales.assign(sale_day=sales["sale_date"].dt.date)
        .groupby(["sale_day", "branch_id"], as_index=False)
        .agg(
            total_sales_amount=("sales_amount", "sum"),
            total_units_sold=("quantity", "sum"),
            transaction_count=("sale_id", "nunique"),
        )
    )

    inventory_snapshot = inventory.merge(
        products[["product_id", "product_name", "category"]],
        on="product_id",
        how="left",
    )
    inventory_snapshot["inventory_health"] = inventory_snapshot.apply(
        lambda row: "reorder_required"
        if row["on_hand_quantity"] <= row["reorder_level"]
        else "healthy",
        axis=1,
    )

    gold_files = [
        _write_parquet(daily_sales, GOLD_DIR / "daily_sales.parquet"),
        _write_parquet(inventory_snapshot, GOLD_DIR / "inventory_snapshot.parquet"),
    ]

    return TransformArtifacts(
        bronze_files=bronze_files,
        silver_files=silver_files,
        gold_files=gold_files,
    )
