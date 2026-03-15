"""ETL: Neon Postgres → Databricks Unity Catalog (workspace.default)

- Static tables: CREATE OR REPLACE TABLE (1 SQL call per table, full replace)
- sales_transactions: COPY INTO from a timestamped Parquet per run.
  Delta tracks which files were already loaded — no watermark file needed.
- Parquet files are kept locally in data/parquet/ for inspection.

Usage: uv run etl_to_databricks.py
"""

import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

NEON_URL = "postgresql://neondb_owner:npg_y5gFCSeLB2pz@ep-snowy-forest-adlwtnc0.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require"
DATABRICKS_PROFILE = "dbc-df3c4dbf-3ef7"
WAREHOUSE_ID = "90f6df4041b446b7"
CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "staging"

PARQUET_DIR = Path("data/parquet")

# Tables fully replaced on every run (1 SQL call each)
STATIC_TABLES = [
    "sales_customers",
    "sales_franchises",
    "sales_suppliers",
    "media_customer_reviews",
    "media_gold_reviews_chunked",
]

# Tables loaded via COPY INTO (Delta auto-tracks which files were ingested)
INCREMENTAL_TABLES = ["sales_transactions"]


def run_sql(statement):
    """Execute SQL on Databricks via the REST API and wait for completion."""
    payload = json.dumps({
        "statement": statement,
        "warehouse_id": WAREHOUSE_ID,
        "wait_timeout": "50s",
    })
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "--json", payload, "--profile", DATABRICKS_PROFILE],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"databricks api post failed:\nstdout: {result.stdout}\nstderr: {result.stderr}")
    resp = json.loads(result.stdout)
    state = resp.get("status", {}).get("state", "UNKNOWN")

    if state == "SUCCEEDED":
        return resp
    if state in ("PENDING", "RUNNING"):
        stmt_id = resp["statement_id"]
        for _ in range(30):
            time.sleep(2)
            poll = subprocess.run(
                ["databricks", "api", "get", f"/api/2.0/sql/statements/{stmt_id}",
                 "--profile", DATABRICKS_PROFILE],
                capture_output=True, text=True, check=True,
            )
            poll_resp = json.loads(poll.stdout)
            poll_state = poll_resp.get("status", {}).get("state")
            if poll_state == "SUCCEEDED":
                return poll_resp
            if poll_state == "FAILED":
                raise RuntimeError(f"SQL failed: {poll_resp}")
        raise RuntimeError(f"SQL timed out: {statement[:100]}...")

    if state == "FAILED":
        error = resp.get("status", {}).get("error", {}).get("message", "unknown error")
        raise RuntimeError(f"SQL failed: {error}\nSQL: {statement[:200]}")

    return resp


def export_and_upload(duck, table: str, parquet_path: Path) -> tuple[int, str]:
    duck.execute(
        f"COPY (SELECT * FROM neon.public.{table}) TO '{parquet_path}' (FORMAT PARQUET)"
    )
    count = duck.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()[0]
    size_kb = parquet_path.stat().st_size / 1024
    print(f"  Exported {count} rows ({size_kb:.0f} KB)")

    remote_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{parquet_path.name}"
    subprocess.run(
        ["databricks", "fs", "cp", str(parquet_path), f"dbfs:{remote_path}",
         "--overwrite", "--profile", DATABRICKS_PROFILE],
        check=True, capture_output=True, text=True,
    )
    print(f"  Uploaded → {parquet_path.name}")
    return count, remote_path


def etl():
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    duck = duckdb.connect()
    duck.execute("INSTALL postgres; LOAD postgres;")
    duck.execute(f"ATTACH '{NEON_URL}' AS neon (TYPE POSTGRES, READ_ONLY);")
    print("✓ Connected to Neon Postgres\n")

    # --- Static tables: CREATE OR REPLACE (1 SQL call per table) ---
    for table in STATIC_TABLES:
        print(f"--- {table} ---")
        count, remote_path = export_and_upload(duck, table, PARQUET_DIR / f"{table}.parquet")
        fqn = f"{CATALOG}.{SCHEMA}.{table}"
        run_sql(f"CREATE OR REPLACE TABLE {fqn} AS SELECT * FROM read_files('{remote_path}')")
        print(f"  ✓ replaced ({count} rows)")

    # --- Incremental tables: COPY INTO (Delta tracks loaded files automatically) ---
    for table in INCREMENTAL_TABLES:
        print(f"--- {table} (incremental) ---")
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        parquet_path = PARQUET_DIR / f"{table}_{run_id}.parquet"

        count, remote_path = export_and_upload(duck, table, parquet_path)

        fqn = f"{CATALOG}.{SCHEMA}.{table}"
        volume_folder = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/"

        # Ensure target table exists on first run
        run_sql(
            f"CREATE TABLE IF NOT EXISTS {fqn} "
            f"AS SELECT * FROM read_files('{remote_path}') WHERE 1=0"
        )
        # COPY INTO skips files it has already loaded (tracked in Delta metadata)
        run_sql(f"""
            COPY INTO {fqn}
            FROM '{volume_folder}'
            FILEFORMAT = PARQUET
            PATTERN = '{table}_*.parquet'
            FORMAT_OPTIONS ('mergeSchema' = 'true')
            COPY_OPTIONS ('mergeSchema' = 'true')
        """)
        print(f"  ✓ COPY INTO complete")

    duck.close()
    print("\n✓ ETL complete — all tables in workspace.default")


if __name__ == "__main__":
    etl()
