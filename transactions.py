"""
Inserts random sales transactions into Neon Postgres every 2 seconds.
Mirrors the schema and value ranges of the existing sales_transactions table.

Usage: uv run transactions.py
       uv run transactions.py --interval 1   # custom interval in seconds
       uv run transactions.py --count 20     # stop after N inserts
"""

import argparse
import random
import signal
import sys
import time

import psycopg2

DB_URL = "postgresql://neondb_owner:npg_y5gFCSeLB2pz@ep-snowy-forest-adlwtnc0.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require"

PRODUCTS = [
    "Austin Almond Biscotti",
    "Golden Gate Ginger",
    "Orchard Oasis",
    "Outback Oatmeal",
    "Pearly Pies",
    "Tokyo Tidbits",
]
UNIT_PRICE = 3
PAYMENT_METHODS = ["visa", "mastercard", "amex"]
CUSTOMER_ID_RANGE = (2_000_000, 2_000_299)
FRANCHISE_ID_RANGE = (3_000_000, 3_000_047)


def new_transaction(next_id: int) -> tuple:
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 50)
    total = quantity * UNIT_PRICE
    payment = random.choice(PAYMENT_METHODS)
    card = str(random.randint(10**15, 10**16 - 1))
    customer_id = random.randint(*CUSTOMER_ID_RANGE)
    franchise_id = random.randint(*FRANCHISE_ID_RANGE)
    return (
        next_id,
        customer_id,
        franchise_id,
        product,
        quantity,
        UNIT_PRICE,
        total,
        payment,
        card,
    )


def run(interval: float, count: int | None):
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()

    # Start from max existing id + 1
    cur.execute("SELECT MAX(transactionid) FROM sales_transactions")
    max_id = cur.fetchone()[0] or 1_003_332
    next_id = int(max_id) + 1

    inserted = 0
    print(f"Inserting transactions starting at id={next_id} (every {interval}s). Ctrl+C to stop.\n")

    def shutdown(sig, frame):
        print(f"\nStopped. Inserted {inserted} transaction(s).")
        cur.close()
        conn.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while count is None or inserted < count:
        row = new_transaction(next_id)
        cur.execute(
            """
            INSERT INTO sales_transactions
                (transactionid, customerid, franchiseid, datetime,
                 product, quantity, unitprice, totalprice, paymentmethod, cardnumber)
            VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s)
            """,
            row,
        )
        print(f"  [{next_id}] {row[3]:25s}  qty={row[4]:3d}  ${row[6]:4d}  {row[7]}")
        next_id += 1
        inserted += 1
        if count is None or inserted < count:
            time.sleep(interval)

    print(f"\nDone. Inserted {inserted} transaction(s).")
    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between inserts")
    parser.add_argument("--count", type=int, default=None, help="Stop after N inserts")
    args = parser.parse_args()
    run(args.interval, args.count)
