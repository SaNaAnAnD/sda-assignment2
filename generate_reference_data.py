"""
SDA Assignment 2 - Reference data generator
Digital Payments Processor: Real-Time Fraud Detection & Gateway Monitoring

Builds the four static reference datasets that the Kafka producers draw from.
Nothing here is streamed directly except merchant_master and customer_persona,
which are loaded once into the merchant_ref and customer_persona topics.

Run:  python generate_reference_data.py
"""

import csv
import json
import random
from datetime import date, timedelta

SEED = 65046
random.seed(SEED)

REF_DATE = date(2026, 8, 31)     # "today" for the simulation
N_CUSTOMERS = 5000
N_MERCHANTS = 5000
OUT = "."

# ----------------------------------------------------------------------------
# 1. CUSTOMER PERSONA STORE  (persona P1)
# ----------------------------------------------------------------------------
# rolling_mean_30d / rolling_sd_30d are the fraud scorer's baseline. The
# 3-sigma rule in fraud_scorer.py compares each incoming txn amount against
# these two columns, so the txn producer MUST sample from the same
# distribution or the rule will fire on everything (or nothing).

SEGMENTS = {
    #  name              weight  mean_lo  mean_hi  sd_ratio_lo  sd_ratio_hi
    "SALARIED_URBAN":   (0.34,     900,    2600,      0.30,        0.55),
    "GIG_WORKER":       (0.18,     350,    1100,      0.40,        0.70),
    "STUDENT":          (0.16,     180,     650,      0.45,        0.75),
    "SMALL_BUSINESS":   (0.20,    2200,    7500,      0.35,        0.60),
    "AFFLUENT":         (0.12,    4500,   18000,      0.25,        0.45),
}

CITIES = [
    ("Mumbai", "MH"), ("Delhi", "DL"), ("Bengaluru", "KA"), ("Hyderabad", "TG"),
    ("Chennai", "TN"), ("Pune", "MH"), ("Kolkata", "WB"), ("Ahmedabad", "GJ"),
    ("Jaipur", "RJ"), ("Lucknow", "UP"), ("Chandigarh", "CH"), ("Kochi", "KL"),
    ("Indore", "MP"), ("Nagpur", "MH"), ("Panaji", "GA"), ("Guwahati", "AS"),
]

CITY_GEO = {
    "Mumbai": (19.076, 72.877), "Delhi": (28.614, 77.209),
    "Bengaluru": (12.972, 77.594), "Hyderabad": (17.385, 78.487),
    "Chennai": (13.083, 80.270), "Pune": (18.520, 73.857),
    "Kolkata": (22.573, 88.364), "Ahmedabad": (23.023, 72.571),
    "Jaipur": (26.912, 75.787), "Lucknow": (26.847, 80.947),
    "Chandigarh": (30.734, 76.779), "Kochi": (9.932, 76.267),
    "Indore": (22.720, 75.858), "Nagpur": (21.146, 79.088),
    "Panaji": (15.491, 73.828), "Guwahati": (26.145, 91.736),
}

seg_names = list(SEGMENTS.keys())
seg_weights = [SEGMENTS[s][0] for s in seg_names]


def device_id():
    return "DVC-" + "".join(random.choice("0123456789abcdef") for _ in range(10))


customers = []
devices = []

for i in range(1, N_CUSTOMERS + 1):
    cid = f"CUST{i:06d}"
    seg = random.choices(seg_names, weights=seg_weights, k=1)[0]
    _, m_lo, m_hi, sd_lo, sd_hi = SEGMENTS[seg]

    mean = round(random.uniform(m_lo, m_hi), 2)
    sd = round(mean * random.uniform(sd_lo, sd_hi), 2)

    city, state = random.choice(CITIES)
    lat, lon = CITY_GEO[city]

    account_age = random.randint(45, 2900)
    txn_count = max(4, int(random.gauss(38, 14)))

    # 1-3 devices per customer; the first is the primary
    n_dev = random.choices([1, 2, 3], weights=[0.55, 0.33, 0.12], k=1)[0]
    cust_devices = []
    for d in range(n_dev):
        did = device_id()
        # ~12% of secondary devices are dormant (>90 days) so that the
        # "unseen device in last 90 days" leg of the fraud rule has real
        # negative-and-positive test material rather than firing on noise.
        if d == 0:
            last_seen_days = random.randint(0, 6)
        elif random.random() < 0.12:
            last_seen_days = random.randint(95, 400)
        else:
            last_seen_days = random.randint(1, 80)

        first_seen_days = last_seen_days + random.randint(30, min(900, account_age))
        first_seen_days = min(first_seen_days, account_age)

        cust_devices.append(did)
        devices.append({
            "customer_id": cid,
            "device_id": did,
            "device_type": random.choices(
                ["ANDROID", "IOS", "WEB"], weights=[0.68, 0.24, 0.08], k=1)[0],
            "is_primary": "Y" if d == 0 else "N",
            "first_seen_date": (REF_DATE - timedelta(days=first_seen_days)).isoformat(),
            "last_seen_date": (REF_DATE - timedelta(days=last_seen_days)).isoformat(),
            "days_since_last_seen": last_seen_days,
        })

    customers.append({
        "customer_id": cid,
        "segment": seg,
        "home_city": city,
        "home_state": state,
        "home_lat": lat,
        "home_lon": lon,
        "primary_device_id": cust_devices[0],
        "known_device_count": n_dev,
        "rolling_mean_30d": mean,
        "rolling_sd_30d": sd,
        "txn_count_30d": txn_count,
        "account_age_days": account_age,
        "kyc_status": random.choices(["FULL", "MIN"], weights=[0.93, 0.07], k=1)[0],
        "last_updated": REF_DATE.isoformat(),
    })

# ----------------------------------------------------------------------------
# 2. MERCHANT MASTER DATA  (persona P2)
# ----------------------------------------------------------------------------
MCC = [
    ("5814", "Fast Food Restaurants",        0.16, 260,  "LOW"),
    ("5411", "Grocery Stores / Supermarkets", 0.14, 850,  "LOW"),
    ("5541", "Service Stations (Fuel)",       0.11, 1400, "LOW"),
    ("4900", "Utilities - Electric/Gas/Water", 0.09, 1900, "LOW"),
    ("5912", "Drug Stores / Pharmacies",      0.08, 520,  "LOW"),
    ("5732", "Electronics Stores",            0.07, 12000, "MEDIUM"),
    ("5651", "Family Clothing Stores",        0.07, 1800, "MEDIUM"),
    ("4121", "Taxicabs / Ride Hailing",       0.07, 240,  "LOW"),
    ("5999", "Miscellaneous Retail",          0.06, 950,  "MEDIUM"),
    ("7011", "Hotels / Lodging",              0.05, 6500, "MEDIUM"),
    ("4722", "Travel Agencies",               0.04, 22000, "HIGH"),
    ("7995", "Betting / Gaming",              0.02, 3200, "HIGH"),
    ("6051", "Quasi-Cash / Wallet Load",      0.02, 4800, "HIGH"),
    ("5967", "Direct Marketing - Inbound",    0.02, 1600, "HIGH"),
]

PREFIX = ["Shree", "Nova", "Anand", "Metro", "Krishna", "Urban", "Sagar", "Vertex",
          "Lakshmi", "Prime", "Kalpana", "Zenith", "Rohan", "Bharat", "Aster", "Nimbus"]
SUFFIX = ["Traders", "Retail", "Enterprises", "Mart", "Stores", "Services",
          "Solutions", "& Co", "Ventures", "Supply", "Hub", "Depot"]

mcc_codes = [m[0] for m in MCC]
mcc_weights = [m[2] for m in MCC]
mcc_lookup = {m[0]: m for m in MCC}

merchants = []
for i in range(1, N_MERCHANTS + 1):
    mid = f"MERCH{i:06d}"
    code = random.choices(mcc_codes, weights=mcc_weights, k=1)[0]
    _, desc, _, avg_ticket, base_risk = mcc_lookup[code]

    onboard_days = random.randint(20, 2200)

    # Risk tier is mostly driven by MCC, nudged by how new the merchant is.
    # New + high-MCC merchants are the classic mule-account pattern, which
    # gives the fraud analyst something defensible to look at.
    risk = base_risk
    if onboard_days < 90 and risk == "LOW":
        risk = "MEDIUM"
    elif onboard_days < 90 and risk == "MEDIUM":
        risk = "HIGH"
    if random.random() < 0.04:
        risk = random.choice(["LOW", "MEDIUM", "HIGH"])

    city, state = random.choice(CITIES)

    merchants.append({
        "merchant_id": mid,
        "merchant_name": f"{random.choice(PREFIX)} {random.choice(SUFFIX)}",
        "mcc": code,
        "mcc_description": desc,
        "city": city,
        "state": state,
        "onboarding_date": (REF_DATE - timedelta(days=onboard_days)).isoformat(),
        "merchant_age_days": onboard_days,
        "risk_tier": risk,
        "avg_ticket_size": round(avg_ticket * random.uniform(0.6, 1.5), 2),
        "settlement_cycle": random.choices(["T+1", "T+2", "T+0"],
                                           weights=[0.72, 0.20, 0.08], k=1)[0],
        "channel": random.choices(["QR_INSTORE", "ONLINE_CHECKOUT", "POS_CARD"],
                                  weights=[0.45, 0.38, 0.17], k=1)[0],
        "is_active": "Y" if random.random() > 0.03 else "N",
    })

# ----------------------------------------------------------------------------
# 3. ACQUIRING BANK REGISTRY  (persona P3)
# ----------------------------------------------------------------------------
# Not in the Assignment 1 source table, but the gateway producer needs a
# baseline to degrade away from. Without it the "success rate fell from 97%
# to 71%" scenario has no 97% to fall from.
banks = [
    {"bank_id": "ACQ_HDFC", "bank_name": "HDFC Bank",   "routing_weight": 0.28,
     "baseline_success_rate": 0.978, "baseline_latency_ms": 210, "latency_sd_ms": 55},
    {"bank_id": "ACQ_ICICI", "bank_name": "ICICI Bank", "routing_weight": 0.24,
     "baseline_success_rate": 0.971, "baseline_latency_ms": 245, "latency_sd_ms": 62},
    {"bank_id": "ACQ_AXIS",  "bank_name": "Axis Bank",  "routing_weight": 0.20,
     "baseline_success_rate": 0.965, "baseline_latency_ms": 280, "latency_sd_ms": 70},
    {"bank_id": "ACQ_SBI",   "bank_name": "State Bank of India", "routing_weight": 0.18,
     "baseline_success_rate": 0.952, "baseline_latency_ms": 340, "latency_sd_ms": 95},
    {"bank_id": "ACQ_YES",   "bank_name": "Yes Bank",   "routing_weight": 0.10,
     "baseline_success_rate": 0.944, "baseline_latency_ms": 310, "latency_sd_ms": 88},
]

# ----------------------------------------------------------------------------
# 4. SIMULATION CONFIG
# ----------------------------------------------------------------------------
# Single source of truth for the producers. Keeps the fraud-injection rate and
# the outage window out of the producer code so they can be tuned without
# editing three scripts.
config = {
    "seed": SEED,
    "reference_date": REF_DATE.isoformat(),
    "kafka": {
        "bootstrap_servers": "localhost:9092",
        "topics": {
            "transactions": {"partitions": 3, "key": "customer_id"},
            "user_sessions": {"partitions": 3, "key": "customer_id"},
            "gateway_status": {"partitions": 3, "key": "bank_id"},
            "merchant_ref": {"partitions": 1, "key": "merchant_id"},
            "customer_persona": {"partitions": 1, "key": "customer_id"},
        },
    },
    "rates": {
        "transactions_per_min": 3000,
        "sessions_per_min": 1200,
        "gateway_events_per_min": 3000,
    },
    "fraud_injection": {
        "rate": 0.015,
        "sigma_multiplier_range": [3.5, 8.0],
        "unseen_device_probability": 0.85,
        "geo_mismatch_probability": 0.60,
        "high_risk_merchant_bias": 0.70,
    },
    "gateway_degradation": {
        "target_bank": "ACQ_AXIS",
        "start_offset_sec": 180,
        "duration_sec": 240,
        "degraded_success_rate": 0.71,
        "degraded_latency_ms": 1450,
    },
    "counts": {
        "customers": N_CUSTOMERS,
        "merchants": N_MERCHANTS,
        "devices": len(devices),
        "banks": len(banks),
    },
}


# ----------------------------------------------------------------------------
# WRITE
# ----------------------------------------------------------------------------
def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"  {path:34s} {len(rows):>6,} rows")


print("Generating reference data (seed=%d)..." % SEED)
write_csv(f"{OUT}/customer_persona.csv", customers)
write_csv(f"{OUT}/merchant_master.csv", merchants)
write_csv(f"{OUT}/device_registry.csv", devices)
write_csv(f"{OUT}/acquiring_banks.csv", banks)

with open(f"{OUT}/sim_config.json", "w", encoding="utf-8") as f:
    json.dump(config, f, indent=2)
print(f"  {OUT}/sim_config.json")

# sanity checks
dormant = sum(1 for d in devices if d["days_since_last_seen"] > 90)
high_risk = sum(1 for m in merchants if m["risk_tier"] == "HIGH")
print("\nSanity:")
print(f"  devices dormant >90d : {dormant:,} ({dormant/len(devices):.1%})")
print(f"  HIGH risk merchants  : {high_risk:,} ({high_risk/len(merchants):.1%})")
print(f"  mean rolling_mean_30d: Rs {sum(c['rolling_mean_30d'] for c in customers)/len(customers):,.0f}")
