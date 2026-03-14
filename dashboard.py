# /// script
# requires-python = ">=3.11"
# dependencies = ["streamlit", "polars", "altair", "pandas", "pyarrow"]
# ///
import subprocess
import streamlit as st
import polars as pl
import altair as alt
import pandas as pd

st.set_page_config(page_title="Sales Dashboard", layout="wide")


def load_data():
    st.sidebar.info("Downloading data from database...")
    subprocess.run(
        [
            "psql",
            "postgresql://neondb_owner:npg_y5gFCSeLB2pz@ep-snowy-forest-adlwtnc0.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require",
            "-c",
            "\\COPY sales_transactions TO 'sales_transactions.csv' WITH CSV HEADER",
        ],
        check=True,
        capture_output=True,
    )
    df = pl.read_csv("sales_transactions.csv")
    return df.with_columns(pl.col("datetime").str.to_datetime())


df = load_data()

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
    st.altair_chart(chart2)

with tab4:
    st.dataframe(df.to_pandas(), use_container_width=True)
