Enterprise E-Commerce Medallion Lakehouse on Azure Databricks

This guide outlines the modular repository structure, notebook decomposition, and architectural documentation for your GitHub portfolio project.

1. Repository Directory Layout

A production-grade lakehouse repository separates infrastructure, pipelines, data quality checks, and documentation:

azure-databricks-ecommerce-lakehouse/
├── README.md
├── architecture_diagram.png
├── adf/
│   └── pipelines/
│       └── pl_copy_s3_to_adls_dataset1.json
├── notebooks/
│   ├── 00_setup_and_governance/
│   │   ├── 00_storage_credentials_and_locations.py
│   │   └── 01_unity_catalog_metastore_setup.py
│   ├── 01_bronze/
│   │   ├── 01_raw_batch_ingestion.py
│   │   └── 02_dead_letter_queue_audit.py
│   ├── 02_silver/
│   │   ├── 01_dim_customers_scd2.py
│   │   ├── 02_dim_products_catalog.py
│   │   ├── 03_fct_orders_fulfillment.py
│   │   └── 04_fct_financial_ledger_cdc.py
│   ├── 03_gold/
│   │   ├── 01_customer_lifetime_value_rfm.py
│   │   ├── 02_fulfillment_logistics_sla.py
│   │   └── 03_daily_financial_reconciliation.py
│   └── 04_orchestration/
│       └── databricks_workflow_job_definition.json
└── tests/
    └── data_quality_expectations.py


2. Notebook Responsibilities & Descriptions

Layer 0: Setup & Governance

00_storage_credentials_and_locations.py

Configures Azure Managed Identity with Unity Catalog Access Connectors.

Registers External Locations for landing, bronze, silver, and gold on ADLS Gen2 (sadataproject01).

01_unity_catalog_metastore_setup.py

Initializes catalog schemas: CREATE SCHEMA IF NOT EXISTS bronze, silver, gold.

Configures row/column-level access control tags and auditing defaults.

Layer 1: Bronze (Raw Append & Audit Metadata)

01_raw_batch_ingestion.py

Reads multi-file batches from landing/raw_feeds/.

Dynamically maps domain files to their target Delta tables.

Injects enterprise audit headers:

_ingested_at: UTC timestamp of arrival.

_source_file_name: Original storage origin (_metadata.file_name).

_batch_id: UUID for end-to-end lineage tracing.

_row_hash: SHA-256 payload fingerprint for deduplication.

02_dead_letter_queue_audit.py

Isolates corrupted CSV lines (malformed commas, schema drifts) into a quarantine table (bronze.dlq_quarantine).

Layer 2: Silver (Conformed, Normalized & CDC Enrichment)

01_dim_customers_scd2.py

Implements Slowly Changing Dimension Type 2 (SCD2) using Spark SQL window functions:

LEAD() across usr_addr_change_date to calculate valid_to.

Default 9999-12-31 sentinel date for active records.

Boolean is_current flag for rapid query filtering.

02_dim_products_catalog.py

Consolidates first-party (catalog, products, brands) and third-party vendor feeds (catalogs_3rd_party, brands_3rd_party).

Resolves brand variations and duplicates using Levenshtein distance metrics.

Pivots key-value EAV rows from products_attribute into structured schema columns.

03_fct_orders_fulfillment.py

Joins orders, order_items, shipment, and shipment_items at the order-line grain.

Computes fulfillment cycle metrics (order-to-shipment SLA latency, dispatch delay).

04_fct_financial_ledger_cdc.py

Merges payment_txn_cdc CDC change logs using Delta MERGE INTO (upserting inserts and updates).

Joins gateway settlement logs (payment_gtw_log) and returns to construct an auditable double-entry accounting ledger.

Layer 3: Gold (Aggregated Business Reporting & Feature Store)

01_customer_lifetime_value_rfm.py

Computes Recency, Frequency, and Monetary (RFM) segmentation per customer.

02_fulfillment_logistics_sla.py

Aggregates carrier performance, on-time delivery rates, and fulfillment center bottlenecks.

03_daily_financial_reconciliation.py

Summarizes gross revenue, coupon discounts, merchant fees, refunds, and net ledger balances for daily financial reporting.

3. GitHub README Template

# Enterprise E-Commerce Lakehouse on Azure Databricks (Serverless)

An end-to-end modern data platform migrating on-premise/AWS e-commerce workloads to Microsoft Azure using the Medallion Architecture, Unity Catalog, Delta Lake, and Azure Data Factory.

## Architecture
1. **Ingestion:** Azure Data Factory cloud-to-cloud binary copy pipeline moving 52 GB of raw operational data from AWS S3 to Azure Data Lake Storage Gen2 (ADLS Gen2).
2. **Bronze Layer:** Delta Lake tables enriched with audit headers (`_ingested_at`, `_source_file_name`, `_batch_id`, `_row_hash`).
3. **Silver Layer:** 
   - SCD Type 2 Customer Dimension tracking historical address transitions.
   - String similarity (Levenshtein) deduplication on third-party catalog feeds.
   - CDC stream processing using Delta Lake `MERGE INTO` upsert mechanics.
4. **Gold Layer:** Business dimensional marts for RFM segmentation, fulfillment SLA analysis, and financial ledger reconciliation.
5. **Governance:** Unity Catalog role-based access control (RBAC), external locations, and lineage tracking.
