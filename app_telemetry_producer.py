"""
app_telemetry_producer.py
-------------------------
Streams mobile-app clickstream events into the Kafka topic 'user_sessions'.

This runs FOREVER until you press Ctrl+C.

This is the simplest of the three producers. It simulates people opening the
app and moving through it: LOGIN, then some screen views, then maybe a
payment attempt, then the session ends.

Why the fraud consumer cares: a transaction arriving with no matching app
session is a signal in its own right. So is a login from a device the
customer has never used, which is often the first thing that happens in an
account-takeover attack - before any money moves.

Run:  python3 app_telemetry_producer.py
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
TOPIC = "user_sessions"
DATA_DIR = os.path.expanduser("~/sda_assignment2/data")

SCREENS = [
    "HOME", "SEND_MONEY", "SCAN_QR", "BILL_PAY", "TXN_HISTORY",
    "REWARDS", "PROFILE", "ADD_BANK", "MOBILE_RECHARGE",
]


def load_config():
    with open(os.path.join(DATA_DIR, "sim_config.json"), encoding="utf-8") as f:
        return json.load(f)


def load_csv(name):
    with open(os.path.join(DATA_DIR, name), newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main():
    cfg = load_config()
    rate_per_min = cfg["rates"]["sessions_per_min"]
    interval = 60.0 / rate_per_min

    print("Loading reference data ...")
    customers = load_csv("customer_persona.csv")
    devices_raw = load_csv("device_registry.csv")

    devices_by_customer = defaultdict(list)
    device_type = {}
    for d in devices_raw:
        devices_by_customer[d["customer_id"]].append(d["device_id"])
        device_type[d["device_id"]] = d["device_type"]

    print(f"  {len(customers):,} customers")
    print(f"\nTarget rate: {rate_per_min:,}/min  ({rate_per_min/60:.0f}/sec)")
    print(f"Connecting to Kafka at {BOOTSTRAP} ...")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=20,
    )

    print("Streaming. Press Ctrl+C to stop.\n")

    sent = 0
    sessions = 0
    started = time.perf_counter()
    next_send = started

    # A session is a short burst of events for one customer. We keep a few
    # sessions open at a time and emit their next event in turn, which is
    # closer to real traffic than one isolated event per customer.
    open_sessions = []

    try:
        while True:
            # Start a new session if we are running low
            if len(open_sessions) < 12 or random.random() < 0.25:
                cust = random.choice(customers)
                cid = cust["customer_id"]
                dev = random.choice(devices_by_customer[cid])
                open_sessions.append({
                    "session_id": "SES" + uuid.uuid4().hex[:14].upper(),
                    "customer_id": cid,
                    "device_id": dev,
                    "city": cust["home_city"],
                    "steps_left": random.randint(2, 7),
                    "started": True,
                })
                sessions += 1

            s = random.choice(open_sessions)

            if s["started"]:
                event_type = "LOGIN"
                screen = "HOME"
                s["started"] = False
            elif s["steps_left"] <= 0:
                event_type = "SESSION_END"
                screen = None
            else:
                event_type = random.choices(
                    ["SCREEN_VIEW", "PAYMENT_INITIATED", "SEARCH"],
                    weights=[0.68, 0.22, 0.10], k=1)[0]
                screen = random.choice(SCREENS)
                s["steps_left"] -= 1

            event = {
                "session_id": s["session_id"],
                "customer_id": s["customer_id"],
                "device_id": s["device_id"],
                "device_type": device_type.get(s["device_id"], "ANDROID"),
                "event_type": event_type,
                "screen": screen,
                "city": s["city"],
                "app_version": random.choices(
                    ["8.4.1", "8.3.7", "8.2.9"], weights=[0.7, 0.22, 0.08], k=1)[0],
                "network": random.choices(
                    ["WIFI", "4G", "5G"], weights=[0.38, 0.34, 0.28], k=1)[0],
                "event_time": datetime.now(timezone.utc).isoformat(),
            }

            producer.send(TOPIC, key=s["customer_id"], value=event)
            sent += 1

            if event_type == "SESSION_END":
                open_sessions.remove(s)

            if sent % 500 == 0:
                elapsed = time.perf_counter() - started
                print(f"  {sent:>7,} events | {sessions:>6,} sessions | "
                      f"{sent/elapsed:>5.0f}/sec")

            next_send += interval
            sleep_for = next_send - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)

    except KeyboardInterrupt:
        elapsed = time.perf_counter() - started
        print(f"\n\nStopped after {elapsed:.0f} seconds.")
        print(f"  Events sent  : {sent:,}")
        print(f"  Sessions      : {sessions:,}")
        print(f"  Average rate  : {sent/max(elapsed,1):.0f}/sec")
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
