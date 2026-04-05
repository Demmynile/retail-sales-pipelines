from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv


def _resolve_project_root() -> Path:
    env_project_root = os.getenv("RETAIL_SALES_PIPELINE_PROJECT_ROOT")
    if env_project_root:
        return Path(env_project_root).expanduser().resolve()

    cwd = Path.cwd().resolve()
    for candidate in (cwd, *cwd.parents):
        if (candidate / "pyproject.toml").exists() and (candidate / "data").exists():
            return candidate

    package_root = Path(__file__).resolve().parents[2]
    return package_root


PROJECT_ROOT = _resolve_project_root()
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
BRONZE_DIR = PROCESSED_DATA_DIR / "bronze"
SILVER_DIR = PROCESSED_DATA_DIR / "silver"
GOLD_DIR = PROCESSED_DATA_DIR / "gold"
REPORTS_DIR = PROJECT_ROOT / "reports"

load_dotenv(PROJECT_ROOT / ".env")


@dataclass(slots=True)
class Settings:
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    aws_access_key_id: str | None = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str | None = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3_bucket: str = os.getenv("S3_BUCKET", "retailio-retail-sales-bronze")
    s3_raw_prefix: str = os.getenv("S3_RAW_PREFIX", "raw")
    s3_silver_prefix: str = os.getenv("S3_SILVER_PREFIX", "silver")
    s3_gold_prefix: str = os.getenv("S3_GOLD_PREFIX", "gold")
    warehouse_target: str = os.getenv("WAREHOUSE_TARGET", "motherduck")
    warehouse_database: str = os.getenv("WAREHOUSE_DATABASE", "retailio")
    warehouse_schema: str = os.getenv("WAREHOUSE_SCHEMA", "retail_data")
    motherduck_database: str = os.getenv("MOTHERDUCK_DATABASE", "retailio")
    motherduck_token: str | None = os.getenv("MOTHERDUCK_TOKEN")
    snowflake_account: str | None = os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_user: str | None = os.getenv("SNOWFLAKE_USER")
    snowflake_password: str | None = os.getenv("SNOWFLAKE_PASSWORD")
    snowflake_warehouse: str | None = os.getenv("SNOWFLAKE_WAREHOUSE")
    snowflake_database: str = os.getenv("SNOWFLAKE_DATABASE", "RETAILIO")
    snowflake_schema: str = os.getenv("SNOWFLAKE_SCHEMA", "RETAIL_DATA")
    snowflake_role: str = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")


def get_settings() -> Settings:
    return Settings()


def ensure_local_directories() -> None:
    for directory in (BRONZE_DIR, SILVER_DIR, GOLD_DIR, REPORTS_DIR):
        directory.mkdir(parents=True, exist_ok=True)
