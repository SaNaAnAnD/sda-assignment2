"""
merchant_loader.py
------------------
Loads merchant_master.csv into the Kafka topic 'merchant_ref'.

Same idea as persona_loader.py: a ONE-SHOT load, not a stream. Reads the
file, pushes every row, exits.

Why this topic exists: a transaction event only carries a merchant_id.
To know whether that merchant is a high-risk travel agency onboarded last
month or a supermarket trading for six years, the consumer needs this
lookup table.

Run:  python3 merchant_loader.py
"""

import csv
import json
import os
from kafka import KafkaProducer

# ---------------------------------------------------------------------
# SETTINGS
# ---------------------------------------------------------------------
BOOTSTRAP = "localhost:9092"
TOPIC = "merchant_ref"
CSV_PATH = os.path.expanduser("~/sda_assignment2/data/merchant_master.csv")

INT_COLS = ["merchant_age_days"]
FLOAT_COLS = ["avg_ticket_size"]


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
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )

    print(f"Reading {CSV_PATH} ...")

    sent = 0
    skipped = 0
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Closed merchants cannot receive payments, so there is no
            # point publishing them. This also gives the assignment a
            # small, defensible filtering step at the load stage.
            if row["is_active"] != "Y":
                skipped += 1
                continue

            row = clean_row(row)
            producer.send(TOPIC, key=row["merchant_id"], value=row)

            sent += 1
            if sent % 1000 == 0:
                print(f"  ... {sent:,} sent")

    producer.flush()
    producer.close()

    print(f"\nDone. {sent:,} active merchants loaded into '{TOPIC}'.")
    print(f"      {skipped:,} inactive merchants skipped.")


if __name__ == "__main__":
    main()
