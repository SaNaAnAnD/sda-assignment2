"""
persona_loader.py
-----------------
Loads customer_persona.csv into the Kafka topic 'customer_persona'.

This is a ONE-SHOT loader, not a stream. It reads the file, pushes every
row into Kafka, prints a summary, and exits. Run it once before starting
the streaming producers.

Why this topic exists: the fraud scorer needs each customer's normal
spending level to decide whether a new transaction is unusual. Putting
that reference data in Kafka means the scorer can load it at startup
instead of querying a database for every single transaction.

Run:  python3 persona_loader.py
"""

import csv
import json
import os
from kafka import KafkaProducer

# ---------------------------------------------------------------------
# SETTINGS
# ---------------------------------------------------------------------
BOOTSTRAP = "localhost:9092"        # where Kafka is listening
TOPIC = "customer_persona"
CSV_PATH = os.path.expanduser("~/sda_assignment2/data/customer_persona.csv")

# Columns that should be numbers, not text. CSV files store everything as
# text, so without this the fraud scorer would try to compare the string
# "2496.44" against a number and crash.
INT_COLS = ["known_device_count", "txn_count_30d", "account_age_days"]
FLOAT_COLS = ["home_lat", "home_lon", "rolling_mean_30d", "rolling_sd_30d"]


def clean_row(row):
    """Convert text values from the CSV into proper numbers."""
    for col in INT_COLS:
        row[col] = int(row[col])
    for col in FLOAT_COLS:
        row[col] = float(row[col])
    return row


def main():
    print(f"Connecting to Kafka at {BOOTSTRAP} ...")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        # Kafka only understands raw bytes, so we tell it how to turn our
        # Python dictionary into bytes: first into JSON text, then encode.
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",          # wait for Kafka to confirm each message landed
        retries=3,
    )

    print(f"Reading {CSV_PATH} ...")

    sent = 0
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row = clean_row(row)

            # The KEY decides which partition the message goes to. Using
            # customer_id means every message about one customer always
            # lands in the same partition, so it stays in order.
            producer.send(TOPIC, key=row["customer_id"], value=row)

            sent += 1
            if sent % 1000 == 0:
                print(f"  ... {sent:,} sent")

    # send() is asynchronous - it queues messages rather than sending them
    # immediately. flush() waits until everything has actually left.
    producer.flush()
    producer.close()

    print(f"\nDone. {sent:,} customer records loaded into '{TOPIC}'.")


if __name__ == "__main__":
    main()
