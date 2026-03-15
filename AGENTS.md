# Agentic Modern Data Project

## Overview
This project analyzes sales transaction data from a PostgreSQL database (Neon) and provides analytics via Python scripts and a Streamlit dashboard.

## Database
- **PostgreSQL (Neon)**: `postgresql://neondb_owner:npg_y5gFCSeLB2pz@ep-snowy-forest-adlwtnc0.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require`
- **Tables**:
  - `sales_transactions` - 3,333 transactions
  - `sales_customers` - 300 customers
  - `sales_franchises` - 48 franchises
  - `sales_suppliers` - 27 suppliers
  - `media_customer_reviews` - 204 reviews
  - `media_gold_reviews_chunked` - 196 chunked reviews

## Scripts

### `analyze_sales.py`
- Downloads data from PostgreSQL to DuckDB
- Generates Altair charts (HTML files)
- Run: `uv run analyze_sales.py`

### `dashboard.py`
- Streamlit dashboard with DuckDB backend
- Shows sales metrics, product revenue, payment methods, time analysis
- Run: `uv run streamlit run dashboard.py`

## Dependencies
Managed via `uv`:
- polars, altair, pandas, pyarrow
- streamlit
- duckdb
- databricks-sql-connector

## Databricks Integration
- Profile: `dbc-df3c4dbf-3ef7`
- Host: `https://dbc-df3c4dbf-3ef7.cloud.databricks.com/`
- Unity Catalog: workspace (workspace.default schema)
- No SQL warehouse configured yet - needs to be created or use serverless endpoint

## DuckDB Storage
- Location: `data/sales.duckdb`
- Tables:
  - `sales_transactions_raw` - with download_id and downloaded_at columns for versioning
  - `download_log` - tracks download timestamps

## Key Files
- `queries.sql` - PostgreSQL queries
- `duckdb_queries.sql` - DuckDB queries
- `data/raw/` - CSV downloads (old, can be cleaned up)
