"""
txn_producer.py
---------------
Streams fake payment authorisation events into the Kafka topic 'transactions'.

This runs FOREVER until you press Ctrl+C.

How it works: it loads the three reference CSVs into memory once at startup,
then in a loop it picks a random customer, looks up how much that customer
normally spends, and invents a transaction around that amount.

Roughly 1.5% of the time it deliberately produces a SUSPICIOUS transaction -
a very large amount, on a phone the customer has never used, often in a
different city. Without these injected anomalies the fraud consumer in
Assignment 3 would run perfectly and detect nothing, which makes for a very
boring screenshot.

Run:  python3 txn_producer.py
Stop: Ctrl+C
"""

import csv
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from collections import defaultdict
from kafka import KafkaProducer

# ---------------------------------------------------------------------
# SETTINGS
# ---------------------------------------------------------------------
BOOTSTRAP = "localhost:9092"
TOPIC = "transactions"
DATA_DIR = os.path.expanduser("~/sda_assignment2/data")
CONFIG_PATH = os.path.join(DATA_DIR, "sim_config.json")


def load_config():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_csv(name):
    with open(os.path.join(DATA_DIR, name), newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def new_device_id():
    """Invent a device the customer has never used before."""
    return "DVC-" + "".join(random.choice("0123456789abcdef") for _ in range(10))


def main():
    cfg = load_config()
    fraud_cfg = cfg["fraud_injection"]
    rate_per_min = cfg["rates"]["transactions_per_min"]
    interval = 60.0 / rate_per_min          # seconds between events

    print("Loading reference data ...")
    customers = load_csv("customer_persona.csv")
    merchants = [m for m in load_csv("merchant_master.csv") if m["is_active"] == "Y"]
    devices_raw = load_csv("device_registry.csv")
    banks = load_csv("acquiring_banks.csv")

    # Group devices by customer so we can pick one of THEIR phones quickly
    devices_by_customer = defaultdict(list)
    for d in devices_raw:
        devices_by_customer[d["customer_id"]].append(d["device_id"])

    # Split merchants by risk so fraud can be biased toward risky ones
    high_risk_merchants = [m for m in merchants if m["risk_tier"] == "HIGH"]
    normal_merchants = [m for m in merchants if m["risk_tier"] != "HIGH"]

    # All the cities in the dataset, for faking a location mismatch
    all_locations = list({
        (c["home_city"], c["home_state"], float(c["home_lat"]), float(c["home_lon"]))
        for c in customers
    })

    bank_ids = [b["bank_id"] for b in banks]
    bank_weights = [float(b["routing_weight"]) for b in banks]

    print(f"  {len(customers):,} customers")
    print(f"  {len(merchants):,} active merchants")
    print(f"  {len(banks)} acquiring banks")
    print(f"\nTarget rate: {rate_per_min:,}/min  ({rate_per_min/60:.0f}/sec)")
    print(f"Fraud injection rate: {fraud_cfg['rate']:.1%}")
    print(f"\nConnecting to Kafka at {BOOTSTRAP} ...")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=20,        # batch briefly for throughput
    )

    print("Streaming. Press Ctrl+C to stop.\n")

    sent = 0
    fraud_sent = 0
    started = time.perf_counter()
    next_send = started

    try:
        while True:
            cust = random.choice(customers)
            cid = cust["customer_id"]
            mean = float(cust["rolling_mean_30d"])
            sd = float(cust["rolling_sd_30d"])

            is_fraud = random.random() < fraud_cfg["rate"]

            if is_fraud:
                # --- SUSPICIOUS TRANSACTION -----------------------------
                # Amount is pushed well beyond the customer's normal range
                lo, hi = fraud_cfg["sigma_multiplier_range"]
                sigma = random.uniform(lo, hi)
                amount = round(mean + sigma * sd, 2)

                # Usually on a phone never seen before
                if random.random() < fraud_cfg["unseen_device_probability"]:
                    device = new_device_id()
                else:
                    device = random.choice(devices_by_customer[cid])

                # Often in a city the customer does not live in
                if random.random() < fraud_cfg["geo_mismatch_probability"]:
                    city, state, lat, lon = random.choice(all_locations)
                else:
                    city, state = cust["home_city"], cust["home_state"]
                    lat, lon = float(cust["home_lat"]), float(cust["home_lon"])

                # Often at a high-risk merchant
                if (random.random() < fraud_cfg["high_risk_merchant_bias"]
                        and high_risk_merchants):
                    merch = random.choice(high_risk_merchants)
                else:
                    merch = random.choice(merchants)

                fraud_sent += 1

            else:
                # --- NORMAL TRANSACTION ---------------------------------
                # Drawn from the customer's own spending distribution, so
                # it will sit comfortably inside their 3-sigma band
                amount = round(max(10.0, random.gauss(mean, sd * 0.6)), 2)
                device = random.choice(devices_by_customer[cid])
                city, state = cust["home_city"], cust["home_state"]
                lat, lon = float(cust["home_lat"]), float(cust["home_lon"])
                merch = random.choice(normal_merchants or merchants)

            event = {
                "txn_id": "TXN" + uuid.uuid4().hex[:16].upper(),
                "customer_id": cid,
                "merchant_id": merch["merchant_id"],
                "mcc": merch["mcc"],
                "amount": amount,
                "currency": "INR",
                "payment_mode": random.choices(
                    ["UPI", "CARD"], weights=[0.72, 0.28], k=1)[0],
                "channel": merch["channel"],
                "device_id": device,
                "city": city,
                "state": state,
                "lat": lat,
                "lon": lon,
                "acquiring_bank": random.choices(
                    bank_ids, weights=bank_weights, k=1)[0],
                "event_time": datetime.now(timezone.utc).isoformat(),
                # Ground-truth label. Useful in Assignment 3 to measure how
                # many injected anomalies the fraud rule actually caught.
                # A real payment switch would obviously not have this field.
                "_injected_anomaly": is_fraud,
            }

            # Key by customer_id so all events for one customer land in the
            # same partition and stay in order - this is what makes the
            # rolling-statistics logic in the consumer valid.
            producer.send(TOPIC, key=cid, value=event)
            sent += 1

            if sent % 500 == 0:
                elapsed = time.perf_counter() - started
                print(f"  {sent:>7,} sent | {fraud_sent:>5,} anomalies "
                      f"({fraud_sent/sent:.2%}) | {sent/elapsed:>5.0f}/sec")

            # Pace the loop to hit the target rate
            next_send += interval
            sleep_for = next_send - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)

    except KeyboardInterrupt:
        elapsed = time.perf_counter() - started
        print(f"\n\nStopped after {elapsed:.0f} seconds.")
        print(f"  Transactions sent : {sent:,}")
        print(f"  Injected anomalies: {fraud_sent:,} ({fraud_sent/max(sent,1):.2%})")
        print(f"  Average rate      : {sent/max(elapsed,1):.0f}/sec")
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
