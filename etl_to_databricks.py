"""ETL: Neon Postgres → Databricks Unity Catalog (workspace.default)

Uses delta-rs (deltalake) to write Arrow data directly to Delta format locally,
then uploads the Delta directory to a Databricks Volume.

- Static tables: overwrite local Delta → upload → CREATE OR REPLACE TABLE USING DELTA LOCATION
- sales_transactions: append new rows only (local Delta is the watermark) → upload
  → table auto-reflects new data via LOCATION (no extra SQL needed after first run)

Usage: uv run etl_to_databricks.py
"""

import json
import subprocess
import time
from pathlib import Path

import duckdb
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake

NEON_URL = "postgresql://neondb_owner:npg_y5gFCSeLB2pz@ep-snowy-forest-adlwtnc0.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require"
DATABRICKS_PROFILE = "dbc-df3c4dbf-3ef7"
WAREHOUSE_ID = "90f6df4041b446b7"
CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "staging"

DELTA_DIR = Path("data/delta")

# Tables fully replaced on every run
STATIC_TABLES = [
    "sales_customers",
    "sales_franchises",
    "sales_suppliers",
    "media_customer_reviews",
    "media_gold_reviews_chunked",
]

# Tables loaded incrementally (append-only, local Delta acts as watermark)
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


def write_and_upload(arrow_tbl, table: str, mode: str) -> str:
    """Write Arrow table to local Delta, upload directory to Volume. Returns volume path."""
    local_path = DELTA_DIR / table
    volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/delta/{table}"

    write_deltalake(str(local_path), arrow_tbl, mode=mode, schema_mode="overwrite")
    size_kb = sum(f.stat().st_size for f in local_path.rglob("*") if f.is_file()) / 1024
    print(f"  Written {arrow_tbl.num_rows} rows → local Delta ({size_kb:.0f} KB total)")

    subprocess.run(
        ["databricks", "fs", "cp", str(local_path), f"dbfs:{volume_path}",
         "--recursive", "--overwrite", "--profile", DATABRICKS_PROFILE],
        check=True, capture_output=True, text=True,
    )
    print(f"  Uploaded → {volume_path}")
    return volume_path


def etl():
    DELTA_DIR.mkdir(parents=True, exist_ok=True)

    duck = duckdb.connect()
    duck.execute("INSTALL postgres; LOAD postgres;")
    duck.execute(f"ATTACH '{NEON_URL}' AS neon (TYPE POSTGRES, READ_ONLY);")
    print("✓ Connected to Neon Postgres\n")

    # --- Static tables: overwrite Delta, register via LOCATION ---
    for table in STATIC_TABLES:
        print(f"--- {table} ---")
        arrow_tbl = duck.execute(f"SELECT * FROM neon.public.{table}").to_arrow_table()
        volume_path = write_and_upload(arrow_tbl, table, mode="overwrite")

        fqn = f"{CATALOG}.{SCHEMA}.{table}"
        run_sql(f"CREATE OR REPLACE TABLE {fqn} USING DELTA LOCATION '{volume_path}'")
        print(f"  ✓ registered ({arrow_tbl.num_rows} rows)")

    # --- Incremental tables: append new rows, local Delta is the watermark ---
    for table in INCREMENTAL_TABLES:
        print(f"--- {table} (incremental) ---")
        local_path = DELTA_DIR / table
        fqn = f"{CATALOG}.{SCHEMA}.{table}"
        volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/delta/{table}"

        if local_path.exists():
            # Use local Delta max timestamp as watermark — no separate file needed
            dt_local = DeltaTable(str(local_path))
            max_ts = pc.max(dt_local.to_pyarrow_table(columns=["datetime"]).column("datetime")).as_py()
            print(f"  Watermark: {max_ts}")
            arrow_tbl = duck.execute(
                f"SELECT * FROM neon.public.{table} WHERE datetime > TIMESTAMP '{max_ts}'"
            ).to_arrow_table()
        else:
            arrow_tbl = duck.execute(f"SELECT * FROM neon.public.{table}").to_arrow_table()

        if arrow_tbl.num_rows == 0:
            print(f"  No new rows")
        else:
            write_and_upload(arrow_tbl, table, mode="append")
            # First run: register the LOCATION table. Subsequent runs: uploading
            # updated _delta_log is enough — LOCATION tables reflect new files automatically.
            run_sql(
                f"CREATE TABLE IF NOT EXISTS {fqn} USING DELTA LOCATION '{volume_path}'"
            )
            print(f"  ✓ {arrow_tbl.num_rows} rows appended")

    duck.close()
    print("\n✓ ETL complete — all tables in workspace.default")


if __name__ == "__main__":
    etl()
