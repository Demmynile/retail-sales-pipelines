from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd

from .config import GOLD_DIR, REPORTS_DIR, SILVER_DIR, ensure_local_directories, get_settings


def _read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def generate_quality_report() -> Path:
    ensure_local_directories()
    settings = get_settings()

    sales = _read_parquet(SILVER_DIR / "sales.parquet")
    customers = _read_parquet(SILVER_DIR / "customers.parquet")
    products = _read_parquet(SILVER_DIR / "products.parquet")
    inventory = _read_parquet(SILVER_DIR / "inventory.parquet")
    daily_sales = _read_parquet(GOLD_DIR / "daily_sales.parquet")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "warehouse_target": settings.warehouse_target,
        "datasets": {
            "sales": {
                "row_count": int(len(sales)),
                "null_product_id": int(sales["product_id"].isna().sum()),
                "null_customer_id": int(sales["customer_id"].isna().sum()),
                "null_sales_amount": int(sales["sales_amount"].isna().sum()),
                "min_sales_amount": float(sales["sales_amount"].min()),
                "max_sales_amount": float(sales["sales_amount"].max()),
            },
            "customers": {"row_count": int(len(customers))},
            "products": {"row_count": int(len(products))},
            "inventory": {
                "row_count": int(len(inventory)),
                "low_stock_records": int((inventory["stock_status"] == "low_stock").sum()),
            },
        },
        "gold": {
            "daily_sales_rows": int(len(daily_sales)),
            "daily_sales_total": float(daily_sales["total_sales_amount"].sum()),
        },
    }

    report_path = REPORTS_DIR / f"quality_report_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report_path
