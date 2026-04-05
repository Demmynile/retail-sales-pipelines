# Pipeline Runbook

## 1. Dataset Preparation

- Place regional branch CSV extracts under `data/raw/products`, `data/raw/customers`, `data/raw/sales`, and `data/raw/inventory`.
- Ensure branch files keep the same column names as the sample data.

## 2. S3 Setup

- Run `python -m retail_sales_pipeline.cli setup-s3`.
- This creates the bucket when missing, enables versioning, and installs lifecycle policies.

## 3. Raw Upload

- Run `python -m retail_sales_pipeline.cli upload-raw`.
- Files are uploaded into `raw/products/`, `raw/customers/`, `raw/sales/`, and `raw/inventory/`.

## 4. Cleansing and Aggregation

- Run `python -m retail_sales_pipeline.cli transform`.
- Bronze parquet files mirror the raw CSV content.
- Silver parquet files normalize types and required fields.
- Gold parquet files contain daily sales summaries and inventory health snapshots.

## 5. Airbyte Configuration

- Create the S3 source from [airbyte/config/source_s3.template.json](airbyte/config/source_s3.template.json).
- Choose either [airbyte/config/destination_motherduck.template.json](airbyte/config/destination_motherduck.template.json) or [airbyte/config/destination_snowflake.template.json](airbyte/config/destination_snowflake.template.json).
- Create the connection from [airbyte/config/connection.template.json](airbyte/config/connection.template.json).
- Schedule the connection daily or adjust the cron expression to match branch upload windows.

## 6. Warehouse Validation

- Create the schema using the appropriate SQL in `sql/warehouse/`.
- Run the validation query pack in `sql/validation/` after the initial load and after schema changes.

## 7. Monitoring and Error Handling

- Review Airbyte sync status and job logs after each scheduled run.
- Capture validation output and store report artifacts under `reports/`.
- Connect Airbyte job failures to your alerting platform of choice, such as Slack, Teams, or email via your orchestration layer.
