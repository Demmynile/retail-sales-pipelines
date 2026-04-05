# Medallion Architecture Diagram

```mermaid
flowchart LR
    A[Regional Branch CSV Exports] --> B[AWS S3 Bronze Layer\nraw/products\nraw/customers\nraw/sales\nraw/inventory]
    B --> C[Python Cleansing Jobs\nnormalize fields\nfix types\nremove bad records]
    C --> D[AWS S3 Silver Layer\ncleansed parquet datasets]
    D --> E[Gold Aggregations\ndaily sales\ninventory health]
    D --> F[Airbyte S3 Source]
    E --> F
    F --> G{Warehouse Target}
    G --> H[MotherDuck\nretail_data schema]
    G --> I[Snowflake\nretail_data schema]
    H --> J[Validation SQL\ncounts, nulls, ranges]
    I --> J
    J --> K[Reports and Monitoring]
    F --> L[Airbyte UI Logs\nand sync health]
```

## Layer Responsibilities

- Bronze: immutable raw branch extracts stored in S3 with versioning enabled
- Silver: cleaned and typed datasets aligned to warehouse schema expectations
- Gold: domain aggregates used for BI dashboards, alerts, and reporting
