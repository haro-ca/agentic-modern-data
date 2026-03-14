# /// script
# requires-python = ">=3.11"
# dependencies = ["streamlit", "polars", "altair", "pandas", "pyarrow", "duckdb"]
# ///
import subprocess
import os
from datetime import datetime
import duckdb
import streamlit as st
import polars as pl
import altair as alt
import pandas as pd

st.set_page_config(page_title="Sales Dashboard", layout="wide")

DB_PATH = "data/sales.duckdb"
DB_URL = "postgresql://neondb_owner:npg_y5gFCSeLB2pz@ep-snowy-forest-adlwtnc0.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require"


def init_db():
    os.makedirs("data", exist_ok=True)
    con = duckdb.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS sales_transactions_raw (
            transactionid TEXT,
            customerid BIGINT,
            franchiseid BIGINT,
            datetime TIMESTAMP,
            product TEXT,
            quantity INTEGER,
            unitprice NUMERIC,
            totalprice NUMERIC,
            paymentmethod TEXT,
            cardnumber TEXT,
            download_id INTEGER,
            downloaded_at TIMESTAMP
        )
    """)
    con.execute(
        "CREATE TABLE IF NOT EXISTS download_log (download_id INTEGER, downloaded_at TIMESTAMP)"
    )
    con.execute("CREATE SEQUENCE IF NOT EXISTS download_id_seq")
    con.close()


def download_data():
    init_db()
    con = duckdb.connect(DB_PATH)
    download_id = con.execute("SELECT nextval('download_id_seq')").fetchone()[0]
    downloaded_at = datetime.now()
    partition = downloaded_at.strftime("%Y%m%d_%H%M%S")

    st.sidebar.info(f"Downloading data (download_id: {download_id})...")

    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        temp_path = f.name

    subprocess.run(
        [
            "psql",
            DB_URL,
            "-c",
            f"\\COPY sales_transactions TO '{temp_path}' WITH CSV HEADER",
        ],
        check=True,
        capture_output=True,
    )

    df = pd.read_csv(temp_path)
    os.unlink(temp_path)

    df["download_id"] = download_id
    df["downloaded_at"] = downloaded_at

    con.execute("INSERT INTO sales_transactions_raw SELECT * FROM df")
    con.execute("INSERT INTO download_log VALUES (?, ?)", [download_id, downloaded_at])
    con.close()

    st.sidebar.success(f"Downloaded: {partition} (id: {download_id})")
    return download_id


def get_latest_download_id():
    con = duckdb.connect(DB_PATH)
    result = con.execute("SELECT MAX(download_id) FROM download_log").fetchone()[0]
    con.close()
    return result


init_db()

st.sidebar.title("Data")

col1, col2 = st.sidebar.columns(2)
if col1.button("Refresh"):
    download_data()
    st.rerun()

download_id = get_latest_download_id()
st.sidebar.markdown(f"**Latest download:** `{download_id}`")

con = duckdb.connect(DB_PATH)
df = con.execute(
    "SELECT * FROM sales_transactions_raw WHERE download_id = ?", [download_id]
).df()
con.close()

df = pl.from_pandas(df)
if df["datetime"].dtype == pl.String:
    df = df.with_columns(pl.col("datetime").str.to_datetime())

st.title("Sales Transactions Dashboard")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Transactions", len(df))
col2.metric("Total Revenue", f"${df['totalprice'].sum():,.2f}")
col3.metric("Avg Transaction", f"${df['totalprice'].mean():.2f}")
col4.metric("Median", f"${df['totalprice'].median():.2f}")

st.divider()

tab1, tab2, tab3, tab4 = st.tabs(["Products", "Payment Methods", "Time", "Raw Data"])

with tab1:
    by_product = (
        df.group_by("product")
        .agg(
            pl.col("totalprice").sum().alias("revenue"),
            pl.col("transactionid").count().alias("transactions"),
        )
        .sort("revenue", descending=True)
        .to_pandas()
    )

    chart = (
        alt.Chart(by_product)
        .mark_bar()
        .encode(
            alt.X("revenue", title="Revenue ($)"),
            alt.Y("product", sort="-x", title="Product"),
            tooltip=["product", "revenue", "transactions"],
        )
        .properties(title="Revenue by Product", height=400)
    )
    st.altair_chart(chart, width="stretch")

    st.dataframe(by_product, hide_index=True)

with tab2:
    by_payment = (
        df.group_by("paymentmethod")
        .agg(
            pl.col("totalprice").sum().alias("revenue"),
            pl.col("transactionid").count().alias("transactions"),
        )
        .sort("revenue", descending=True)
        .to_pandas()
    )

    col1, col2 = st.columns(2)
    with col1:
        chart = (
            alt.Chart(by_payment)
            .mark_arc(innerRadius=50)
            .encode(
                alt.Theta("revenue"),
                alt.Color("paymentmethod"),
                tooltip=["paymentmethod", "revenue", "transactions"],
            )
            .properties(title="Revenue by Payment Method", height=350)
        )
        st.altair_chart(chart, width="stretch")
    with col2:
        st.dataframe(by_payment, hide_index=True)

with tab3:
    df_hour = df.with_columns(pl.col("datetime").dt.hour().alias("hour"))
    by_hour = (
        df_hour.group_by("hour")
        .agg(pl.col("transactionid").count().alias("transactions"))
        .sort("hour")
        .to_pandas()
    )

    chart = (
        alt.Chart(by_hour)
        .mark_line(point=True)
        .encode(
            alt.X("hour", title="Hour of Day"),
            alt.Y("transactions", title="Transactions"),
            tooltip=["hour", "transactions"],
        )
        .properties(title="Transactions by Hour", height=300)
    )
    st.altair_chart(chart, width="stretch")

    df_day = df.with_columns(pl.col("datetime").dt.weekday().alias("day"))
    by_day = (
        df_day.group_by("day")
        .agg(pl.col("transactionid").count().alias("transactions"))
        .sort("day")
        .to_pandas()
    )
    day_names = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
    by_day["day_name"] = by_day["day"].map(day_names)

    chart2 = (
        alt.Chart(by_day)
        .mark_bar()
        .encode(
            alt.X(
                "day_name",
                title="Day of Week",
                sort=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
            ),
            alt.Y("transactions", title="Transactions"),
            tooltip=["day_name", "transactions"],
        )
        .properties(title="Transactions by Day of Week", height=300)
    )
    st.altair_chart(chart2, width="stretch")

with tab4:
    st.dataframe(df.to_pandas(), use_container_width=True)
