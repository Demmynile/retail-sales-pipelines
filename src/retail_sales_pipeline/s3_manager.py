from __future__ import annotations

from dataclasses import dataclass
import json
import mimetypes
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from .config import RAW_DATA_DIR, get_settings


DATASET_PREFIXES = {
    "products": "raw/products",
    "customers": "raw/customers",
    "sales": "raw/sales",
    "inventory": "raw/inventory",
}


@dataclass(slots=True)
class UploadResult:
    local_path: Path
    s3_key: str


def _build_s3_client():
    settings = get_settings()
    return boto3.client(
        "s3",
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )


def ensure_bucket_exists() -> None:
    settings = get_settings()
    client = _build_s3_client()

    try:
        client.head_bucket(Bucket=settings.s3_bucket)
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code not in {"404", "NoSuchBucket"}:
            raise

        create_kwargs = {"Bucket": settings.s3_bucket}
        if settings.aws_region != "us-east-1":
            create_kwargs["CreateBucketConfiguration"] = {
                "LocationConstraint": settings.aws_region,
            }
        client.create_bucket(**create_kwargs)


def enable_versioning() -> None:
    settings = get_settings()
    client = _build_s3_client()
    client.put_bucket_versioning(
        Bucket=settings.s3_bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )


def apply_lifecycle_policy() -> None:
    settings = get_settings()
    client = _build_s3_client()
    lifecycle_configuration = {
        "Rules": [
            {
                "ID": "transition-bronze-to-infrequent-access",
                "Filter": {"Prefix": settings.s3_raw_prefix},
                "Status": "Enabled",
                "Transitions": [{"Days": 30, "StorageClass": "STANDARD_IA"}],
                "Expiration": {"Days": 365},
                "NoncurrentVersionTransitions": [
                    {"NoncurrentDays": 30, "StorageClass": "STANDARD_IA"}
                ],
                "NoncurrentVersionExpiration": {"NoncurrentDays": 180},
            },
            {
                "ID": "archive-silver-and-gold",
                "Filter": {"Prefix": settings.s3_silver_prefix},
                "Status": "Enabled",
                "Transitions": [{"Days": 60, "StorageClass": "GLACIER_IR"}],
                "Expiration": {"Days": 540},
            },
            {
                "ID": "archive-gold-layer",
                "Filter": {"Prefix": settings.s3_gold_prefix},
                "Status": "Enabled",
                "Transitions": [{"Days": 90, "StorageClass": "GLACIER_IR"}],
                "Expiration": {"Days": 730},
            },
        ]
    }
    client.put_bucket_lifecycle_configuration(
        Bucket=settings.s3_bucket,
        LifecycleConfiguration=lifecycle_configuration,
    )


def write_bucket_configuration_report() -> str:
    settings = get_settings()
    report = {
        "bucket": settings.s3_bucket,
        "region": settings.aws_region,
        "versioning": "enabled",
        "prefixes": DATASET_PREFIXES,
    }
    return json.dumps(report, indent=2)


def upload_raw_files() -> list[UploadResult]:
    settings = get_settings()
    client = _build_s3_client()
    results: list[UploadResult] = []

    for dataset_name, prefix in DATASET_PREFIXES.items():
        dataset_dir = RAW_DATA_DIR / dataset_name
        for file_path in sorted(dataset_dir.glob("*.csv")):
            s3_key = f"{prefix}/{file_path.name}"
            content_type, _ = mimetypes.guess_type(file_path.name)
            extra_args = {"ContentType": content_type or "text/csv"}
            client.upload_file(
                str(file_path),
                settings.s3_bucket,
                s3_key,
                ExtraArgs=extra_args,
            )
            results.append(UploadResult(local_path=file_path, s3_key=s3_key))
    return results
