from __future__ import annotations

import argparse
import sys

from .reporting import generate_quality_report
from .s3_manager import (
    apply_lifecycle_policy,
    enable_versioning,
    ensure_bucket_exists,
    upload_raw_files,
    write_bucket_configuration_report,
)
from .transformations import build_medallion_layers


def setup_s3() -> None:
    ensure_bucket_exists()
    enable_versioning()
    apply_lifecycle_policy()
    print(write_bucket_configuration_report())


def upload_raw() -> None:
    results = upload_raw_files()
    for result in results:
        print(f"uploaded {result.local_path.name} -> {result.s3_key}")


def transform() -> None:
    artifacts = build_medallion_layers()
    for group_name, paths in {
        "bronze": artifacts.bronze_files,
        "silver": artifacts.silver_files,
        "gold": artifacts.gold_files,
    }.items():
        for path in paths:
            print(f"built {group_name} asset {path}")


def report() -> None:
    report_path = generate_quality_report()
    print(f"report written to {report_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Retail sales integration pipeline utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("setup-s3", help="Create the S3 bucket and configure policies")
    subparsers.add_parser("upload-raw", help="Upload local CSV datasets to the S3 Bronze layer")
    subparsers.add_parser("transform", help="Create local Bronze, Silver, and Gold datasets")
    subparsers.add_parser("report", help="Generate a quality monitoring report")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    command_map = {
        "setup-s3": setup_s3,
        "upload-raw": upload_raw,
        "transform": transform,
        "report": report,
    }
    command_map[args.command]()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
