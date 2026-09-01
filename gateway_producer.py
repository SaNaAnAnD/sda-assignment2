"""
gateway_producer.py
-------------------
Streams acquiring-bank responses into the Kafka topic 'gateway_status'.

This one is different from the other two producers: it is a CONSUMER and a
PRODUCER at the same time. It reads each transaction off the 'transactions'
topic and emits the matching bank response - success or failure, plus how
many milliseconds the bank took.

That mirrors reality. A gateway response cannot exist without a transaction
to respond to, and Assignment 1 states the two streams are one-to-one.

THE OUTAGE: a few minutes after startup, one bank (Axis by default) is
scripted to degrade - its success rate collapses from ~97% to ~71% and its
latency jumps to over a second. It recovers on its own a few minutes later.
This is the event the Payments Operations Manager persona is supposed to
detect and act on, so without it your dashboard has nothing to show.

Run:  python3 gateway_producer.py
Stop: Ctrl+C

IMPORTANT: start txn_producer.py FIRST, in a separate terminal. This script
has nothing to read until transactions are flowing.
"""

import csv
import json
import os
import random
import time
from datetime import datetime, timezone
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer

# ---------------------------------------------------------------------
# SETTINGS
# ---------------------------------------------------------------------
BOOTSTRAP = "localhost:9092"
SOURCE_TOPIC = "transactions"
TARGET_TOPIC = "gateway_status"
DATA_DIR = os.path.expanduser("~/sda_assignment2/data")

# Response codes a bank can return when it declines
FAILURE_CODES = [
    ("51", "INSUFFICIENT_FUNDS", 0.42),
    ("05", "DO_NOT_HONOR", 0.20),
    ("91", "ISSUER_UNAVAILABLE", 0.16),
    ("96", "SYSTEM_MALFUNCTION", 0.12),
    ("57", "TXN_NOT_PERMITTED", 0.10),
]
FAIL_CODES = [f[0] for f in FAILURE_CODES]
FAIL_DESCS = {f[0]: f[1] for f in FAILURE_CODES}
FAIL_WEIGHTS = [f[2] for f in FAILURE_CODES]


def load_config():
    with open(os.path.join(DATA_DIR, "sim_config.json"), encoding="utf-8") as f:
        return json.load(f)


def load_banks():
    with open(os.path.join(DATA_DIR, "acquiring_banks.csv"),
              newline="", encoding="utf-8") as f:
        return {b["bank_id"]: b for b in csv.DictReader(f)}


def main():
    cfg = load_config()
    deg = cfg["gateway_degradation"]
    banks = load_banks()

    print("Acquiring banks loaded:")
    for bid, b in banks.items():
        print(f"  {bid:10s} {b['bank_name']:22s} "
              f"baseline success {float(b['baseline_success_rate']):.1%}")

    print(f"\nScripted outage: {deg['target_bank']} degrades to "
          f"{deg['degraded_success_rate']:.0%} success")
    print(f"  starts at T+{deg['start_offset_sec']}s, "
          f"lasts {deg['duration_sec']}s\n")

    print(f"Connecting to Kafka at {BOOTSTRAP} ...")

    consumer = KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",     # only respond to NEW transactions
        group_id="gateway_producer_v1",
        enable_auto_commit=True,
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=20,
    )

    print("Waiting for transactions ... (start txn_producer.py if idle)\n")

    started = time.perf_counter()
    sent = 0
    per_bank = defaultdict(lambda: {"total": 0, "ok": 0})
    outage_announced = False
    recovery_announced = False

    try:
        for msg in consumer:
            txn = msg.value
            bank_id = txn["acquiring_bank"]
            bank = banks[bank_id]

            elapsed = time.perf_counter() - started
            in_outage = (
                bank_id == deg["target_bank"]
                and deg["start_offset_sec"] <= elapsed
                < deg["start_offset_sec"] + deg["duration_sec"]
            )

            if in_outage:
                if not outage_announced:
                    print(f"\n  *** T+{elapsed:.0f}s  OUTAGE BEGINS on "
                          f"{bank_id} ({bank['bank_name']}) ***\n")
                    outage_announced = True
                success_rate = deg["degraded_success_rate"]
                mean_latency = deg["degraded_latency_ms"]
                latency_sd = mean_latency * 0.35
            else:
                if outage_announced and not recovery_announced \
                        and elapsed >= deg["start_offset_sec"] + deg["duration_sec"]:
                    print(f"\n  *** T+{elapsed:.0f}s  {deg['target_bank']} "
                          f"RECOVERED ***\n")
                    recovery_announced = True
                success_rate = float(bank["baseline_success_rate"])
                mean_latency = float(bank["baseline_latency_ms"])
                latency_sd = float(bank["latency_sd_ms"])

            approved = random.random() < success_rate
            latency = max(35, int(random.gauss(mean_latency, latency_sd)))

            if approved:
                status, code, desc = "SUCCESS", "00", "APPROVED"
            else:
                # During an outage, failures skew toward infrastructure
                # codes rather than customer-side ones like low balance
                if in_outage and random.random() < 0.75:
                    code = random.choice(["91", "96"])
                else:
                    code = random.choices(FAIL_CODES, weights=FAIL_WEIGHTS, k=1)[0]
                desc = FAIL_DESCS[code]
                status = "TIMEOUT" if code == "91" else "FAILURE"
                if status == "TIMEOUT":
                    latency = max(latency, 3000)

            event = {
                "txn_id": txn["txn_id"],
                "customer_id": txn["customer_id"],
                "bank_id": bank_id,
                "bank_name": bank["bank_name"],
                "status": status,
                "response_code": code,
                "response_desc": desc,
                "latency_ms": latency,
                "amount": txn["amount"],
                "payment_mode": txn["payment_mode"],
                "event_time": datetime.now(timezone.utc).isoformat(),
            }

            # Key by bank_id so the health monitor can compute per-bank
            # windowed statistics without shuffling data between partitions
            producer.send(TARGET_TOPIC, key=bank_id, value=event)

            sent += 1
            per_bank[bank_id]["total"] += 1
            if approved:
                per_bank[bank_id]["ok"] += 1

            if sent % 500 == 0:
                parts = []
                for bid in sorted(per_bank):
                    s = per_bank[bid]
                    parts.append(f"{bid.replace('ACQ_',''):5s} "
                                 f"{s['ok']/s['total']:.0%}")
                print(f"  T+{elapsed:>4.0f}s {sent:>7,} responses | "
                      + " | ".join(parts))

    except KeyboardInterrupt:
        elapsed = time.perf_counter() - started
        print(f"\n\nStopped after {elapsed:.0f} seconds.")
        print(f"  Gateway responses sent: {sent:,}\n")
        print("  Per-bank success rate (whole run):")
        for bid in sorted(per_bank):
            s = per_bank[bid]
            print(f"    {bid:10s} {banks[bid]['bank_name']:22s} "
                  f"{s['ok']/max(s['total'],1):>6.1%}  ({s['total']:,} txns)")
        producer.flush()
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
