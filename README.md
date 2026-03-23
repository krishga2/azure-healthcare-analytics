# Azure Healthcare Analytics Platform

An end-to-end Azure data engineering project processing CMS hospital quality data using a full medallion lakehouse architecture.

## Architecture
```
CMS Public API → Azure Data Factory → ADLS Gen2 Bronze
     → PySpark Silver (cleaned) → PySpark Gold (aggregated)
          → Azure Synapse Analytics → Power BI Dashboards
```

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2 |
| Transformation | PySpark — Bronze, Silver, Gold medallion |
| Analytics | Azure Synapse Analytics (Serverless SQL) |
| Visualization | Power BI |
| CI/CD | GitHub + Azure DevOps |
| Governance | RBAC, Azure Key Vault |

## Dataset

- Source: CMS Hospital General Information (public)
- Records: 4,493 hospitals across all US states
- Fields: 38 columns including ratings, ownership, type, emergency services

## Key Results

| Metric | Value |
|---|---|
| Total hospitals processed | 4,493 |
| States covered | 51 |
| Top state by count | California (378) |
| Highest rated ownership | Veterans Health Administration (4.18 avg) |
| Five-star hospitals | 225 |

## Medallion Architecture

- Bronze — Raw CSV from CMS API, stored as-is
- Silver — Cleaned, deduplicated, type-cast, null-handled
- Gold — Three aggregated tables: by State, Hospital Type, Ownership

## Project Structure
```
azure-healthcare-analytics/
├── notebooks/
│   ├── 01_bronze_to_silver.py
│   └── 02_silver_to_gold.py
├── pipelines/
│   └── adf_pipeline_config.json
├── infrastructure/
│   └── setup_commands.sh
└── README.md
```

## How to Run

1. Clone this repo
2. Set up Azure resources using infrastructure/setup_commands.sh
3. Run notebooks in order
4. Connect Synapse to Gold layer for SQL queries
5. Connect Power BI to Synapse for dashboards

## Author

Krishna Gattu — Senior Data Engineer
6+ years experience in Azure, Databricks, Snowflake, Palantir Foundry
