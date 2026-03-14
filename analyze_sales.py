# /// script
# requires-python = ">=3.11"
# dependencies = ["polars", "altair", "pandas", "pyarrow"]
# ///
import subprocess
import polars as pl
import altair as alt
import pandas as pd


def main():
    print("Downloading sales_transactions.csv from database...")
    subprocess.run(
        [
            "psql",
            "postgresql://neondb_owner:npg_y5gFCSeLB2pz@ep-snowy-forest-adlwtnc0.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require",
            "-c",
            "\\COPY sales_transactions TO 'sales_transactions.csv' WITH CSV HEADER",
        ],
        check=True,
    )
    print("Download complete.\n")

    df = pl.read_csv("sales_transactions.csv")

    print("=== Sales Transactions Analysis ===\n")
    print(f"Total transactions: {len(df)}")
    print(f"Total revenue: ${df['totalprice'].sum():.2f}")
    print(f"Average transaction: ${df['totalprice'].mean():.2f}")
    print(f"Median transaction: ${df['totalprice'].median():.2f}")
    print(f"Min: ${df['totalprice'].min():.2f}, Max: ${df['totalprice'].max():.2f}")

    print("\n=== Revenue by Product ===")
    by_product = (
        df.group_by("product")
        .agg(
            pl.col("totalprice").sum().alias("revenue"),
            pl.col("transactionid").count().alias("transactions"),
        )
        .sort("revenue", descending=True)
    )
    print(by_product.head(10).to_pandas().to_string(index=False))

    print("\n=== Revenue by Payment Method ===")
    by_payment = (
        df.group_by("paymentmethod")
        .agg(
            pl.col("totalprice").sum().alias("revenue"),
            pl.col("transactionid").count().alias("transactions"),
        )
        .sort("revenue", descending=True)
    )
    print(by_payment.to_pandas().to_string(index=False))

    print("\n=== Hourly Distribution ===")
    df_with_hour = df.with_columns(
        pl.col("datetime").str.to_datetime().dt.hour().alias("hour")
    )
    by_hour = (
        df_with_hour.group_by("hour")
        .agg(pl.col("transactionid").count().alias("transactions"))
        .sort("hour")
    )
    print(by_hour.to_pandas().to_string(index=False))

    print("\n=== Generating Altair Charts ===")

    chart1 = (
        alt.Chart(df.to_pandas())
        .mark_bar()
        .encode(
            alt.X("totalprice", bin=alt.Bin(maxbins=20), title="Transaction Total"),
            alt.Y("count()", title="Frequency"),
            tooltip=["count()"],
        )
        .properties(title="Distribution of Transaction Totals", width=600, height=300)
    )
    chart1.save("chart_transactions_histogram.html")
    print("Saved: chart_transactions_histogram.html")

    chart2 = (
        alt.Chart(by_product.head(10).to_pandas())
        .mark_bar()
        .encode(
            alt.X("revenue", title="Revenue"),
            alt.Y("product", sort="-x", title="Product"),
            tooltip=["product", "revenue", "transactions"],
        )
        .properties(title="Top 10 Products by Revenue", width=600, height=300)
    )
    chart2.save("chart_top_products.html")
    print("Saved: chart_top_products.html")

    chart3 = (
        alt.Chart(by_payment.to_pandas())
        .mark_arc(innerRadius=50)
        .encode(
            alt.Theta("revenue", title="Revenue"),
            alt.Color("paymentmethod", title="Payment Method"),
            tooltip=["paymentmethod", "revenue", "transactions"],
        )
        .properties(title="Revenue by Payment Method", width=400, height=400)
    )
    chart3.save("chart_payment_methods.html")
    print("Saved: chart_payment_methods.html")

    chart4 = (
        alt.Chart(by_hour.to_pandas())
        .mark_line(point=True)
        .encode(
            alt.X("hour", title="Hour of Day"),
            alt.Y("transactions", title="Number of Transactions"),
            tooltip=["hour", "transactions"],
        )
        .properties(title="Transactions by Hour of Day", width=600, height=300)
    )
    chart4.save("chart_hourly.html")
    print("Saved: chart_hourly.html")

    print("\n=== Analysis Complete ===")


if __name__ == "__main__":
    main()
