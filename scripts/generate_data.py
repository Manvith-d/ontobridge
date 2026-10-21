"""Generate OntoBridge's 12 synthetic sources (4 domains x 3 systems) and the gold alignment key.

Column names are deliberately messy (abbreviations, camelCase, nested JSON, ETL noise columns),
the way real operational extracts look. Everything is seeded, so the files are reproducible:

    python scripts/generate_data.py
"""

from __future__ import annotations

import csv
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "data" / "sources"
rng = random.Random(7374)

FIRST = ["Ava", "Liam", "Maya", "Noah", "Zara", "Ethan", "Priya", "Lucas", "Sofia", "Arjun", "Grace", "Omar",
         "Chloe", "Ravi", "Elena", "Mateo", "Hana", "Kofi", "Isla", "Wei"]
LAST = ["Patel", "Nguyen", "Garcia", "Smith", "Kim", "Okafor", "Rossi", "Chen", "Silva", "Brown", "Reddy",
        "Cohen", "Murphy", "Haddad", "Larsen", "Ito"]
STATES = ["MA", "NY", "IL", "TX", "CA", "WA", "GA", "OH", "NC", "AZ"]
CITIES = {"MA": "Boston", "NY": "Albany", "IL": "Chicago", "TX": "Austin", "CA": "San Diego", "WA": "Seattle",
          "GA": "Atlanta", "OH": "Columbus", "NC": "Raleigh", "AZ": "Phoenix"}
COUNTRIES = ["US", "US", "US", "CA", "GB"]


def name() -> str:
    return f"{rng.choice(FIRST)} {rng.choice(LAST)}"


def day(start: date, span_days: int) -> date:
    return start + timedelta(days=rng.randrange(span_days))


def stamp(start: date, span_days: int) -> str:
    d = datetime.combine(day(start, span_days), datetime.min.time()) + timedelta(seconds=rng.randrange(86400))
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


def money(lo: float, hi: float) -> float:
    return round(rng.uniform(lo, hi), 2)


def zip5() -> str:
    return f"{rng.randrange(1000, 99999):05d}"


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def write_json(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=1))


def main() -> None:
    n = 200
    customers = [f"C{100000 + i}" for i in range(120)]
    agents = [f"AG-{300 + i}" for i in range(25)]
    policies = [f"POL-{700000 + i}" for i in range(n)]

    # ---- Insurance --------------------------------------------------------------------------
    write_csv(SRC / "insurance/policy_admin.csv", [{
        "POL_NO": policies[i], "HOLDER_ID": rng.choice(customers), "HOLDER_NM": name(),
        "EFF_DT": (eff := day(date(2023, 1, 1), 700)).isoformat(),
        "EXP_DT": (eff + timedelta(days=365)).isoformat(), "PREM_AMT": money(400, 4200),
        "LOB_CD": rng.choice(["AUTO", "HOME", "RENTERS", "UMBRELLA"]), "ST_CD": rng.choice(STATES),
        "AGT_ID": rng.choice(agents), "ETL_BATCH_ID": f"b{rng.randrange(1000, 9999)}",
    } for i in range(n)])
    write_json(SRC / "insurance/claims_events.json", [{
        "claimId": f"CLM-{900000 + i}", "policy": {"number": rng.choice(policies)},
        "lossDate": (loss := day(date(2023, 3, 1), 600)).isoformat(),
        "reportedAt": f"{(loss + timedelta(days=rng.randrange(0, 9))).isoformat()}T{rng.randrange(8, 20):02d}:15:00Z",
        "claimant": {"customerRef": rng.choice(customers), "fullName": name()},
        "reserveAmount": money(500, 25000), "paidAmount": money(0, 18000),
        "status": rng.choice(["OPEN", "CLOSED", "REOPENED", "SUBROGATION"]),
        "lossCause": rng.choice(["collision", "hail", "theft", "water", "fire"]),
        "location": {"zip": zip5(), "state": rng.choice(STATES)}, "_ingest_ts": stamp(date(2025, 1, 1), 30),
    } for i in range(n)])
    write_csv(SRC / "insurance/agents_crm.csv", [{
        "agent_code": a, "agent_full_name": (nm := name()), "agency": f"{rng.choice(LAST)} Insurance Group",
        "region": rng.choice(["Northeast", "Midwest", "South", "West"]),
        "hire_date": day(date(2012, 1, 1), 4000).isoformat(),
        "email": f"{nm.split()[0].lower()}.{a.lower()}@agency.example",
    } for a in agents])

    # ---- Retail -----------------------------------------------------------------------------
    skus = [f"SKU-{40000 + i}" for i in range(60)]
    write_csv(SRC / "retail/pos_sales.csv", [{
        "txn_id": f"T{5000000 + i}", "store_no": f"S{rng.randrange(10, 40)}", "sku": rng.choice(skus),
        "qty": rng.randrange(1, 6), "unit_price": money(3, 180), "disc_amt": money(0, 12),
        "txn_ts": stamp(date(2024, 1, 1), 365), "cust_loyalty_id": rng.choice(customers), "cashier": f"emp{rng.randrange(1, 60)}",
    } for i in range(n)])
    write_json(SRC / "retail/ecommerce_orders.json", [{
        "order_id": f"ORD-{200000 + i}",
        "customer": {"id": (c := rng.choice(customers)), "email": f"{c.lower()}@mail.example", "name": name()},
        "placed_at": stamp(date(2024, 1, 1), 365), "items_total": money(15, 650), "shipping_fee": money(0, 25),
        "ship_to": {"postal_code": zip5(), "country": rng.choice(COUNTRIES)},
        "channel": rng.choice(["web", "ios", "android", "marketplace"]),
    } for i in range(n)])
    write_csv(SRC / "retail/product_catalog.csv", [{
        "product_sku": s, "product_title": f"{rng.choice(['Trail', 'Studio', 'Metro', 'Coast'])} {rng.choice(['Jacket', 'Lamp', 'Kettle', 'Backpack', 'Speaker'])}",
        "category_lvl1": rng.choice(["Apparel", "Home", "Kitchen", "Outdoor", "Electronics"]),
        "brand_name": rng.choice(["Northwind", "Contoso", "Fabrikam", "Tailspin"]), "list_price": money(10, 260),
        "launch_dt": day(date(2019, 1, 1), 1800).isoformat(),
    } for s in skus])

    # ---- Healthcare -------------------------------------------------------------------------
    mrns = [f"MRN{8000000 + i}" for i in range(110)]
    npis = [f"{1000000000 + rng.randrange(10**8, 9 * 10**8)}" for _ in range(30)]
    write_json(SRC / "healthcare/ehr_encounters.json", [{
        "encounterId": f"ENC-{600000 + i}",
        "patient": {"mrn": rng.choice(mrns), "dob": day(date(1940, 1, 1), 25000).isoformat(), "sex": rng.choice(["F", "M", "X"])},
        "admitDate": (adm := day(date(2024, 1, 1), 330)).isoformat(),
        "dischargeDate": (adm + timedelta(days=rng.randrange(0, 8))).isoformat(),
        "encounterType": rng.choice(["inpatient", "outpatient", "emergency"]),
        "primaryDx": rng.choice(["E11.9", "I10", "J45.909", "M54.5", "K21.9", "F41.1"]),
        "attendingNpi": rng.choice(npis), "facility": {"code": f"FAC{rng.randrange(1, 9)}", "state": rng.choice(STATES)},
        "totalCharges": money(150, 48000),
    } for i in range(n)])
    write_csv(SRC / "healthcare/pharmacy_fills.csv", [{
        "rx_id": f"RX{3000000 + i}", "patient_mrn": rng.choice(mrns),
        "ndc_code": f"{rng.randrange(10000, 99999)}-{rng.randrange(1000, 9999)}-{rng.randrange(10, 99)}",
        "fill_dt": day(date(2024, 1, 1), 330).isoformat(), "days_supply": rng.choice([7, 14, 30, 60, 90]),
        "qty_dispensed": rng.choice([14, 28, 30, 60, 90]), "copay_amt": money(0, 75), "prescriber_npi": rng.choice(npis),
    } for i in range(n)])
    write_csv(SRC / "healthcare/provider_directory.csv", [{
        "npi": p, "provider_name": f"Dr. {name()}",
        "specialty": rng.choice(["Cardiology", "Family Medicine", "Pulmonology", "Orthopedics", "Psychiatry"]),
        "practice_zip": zip5(), "practice_state": rng.choice(STATES), "accepting_new": rng.choice(["true", "false"]),
    } for p in npis])

    # ---- Finance ----------------------------------------------------------------------------
    accounts = ["1000", "1200", "2000", "4000", "5100", "6100", "6400", "7200"]
    entries = []
    for i in range(n):
        amount, is_debit = money(10, 9000), rng.random() < 0.5
        entries.append({
            "je_id": f"JE-{110000 + i}", "acct_no": rng.choice(accounts),
            "posting_date": day(date(2024, 1, 1), 365).isoformat(),
            "debit_amt": amount if is_debit else 0.0, "credit_amt": 0.0 if is_debit else amount,
            "cost_center": f"CC-{rng.randrange(100, 140)}",
            "currency_cd": rng.choice(["USD", "USD", "CAD", "EUR"]), "memo": rng.choice(["accrual", "reclass", "month-end", ""]),
        })
    write_csv(SRC / "finance/ledger_entries.csv", entries)
    write_json(SRC / "finance/card_transactions.json", [{
        "txnRef": f"AUTH{rng.randrange(10**8, 10**9)}",
        "card": {"last4": f"{rng.randrange(0, 9999):04d}", "holderId": rng.choice(customers)},
        "auth_ts": stamp(date(2024, 1, 1), 365),
        "amount": {"value": money(2, 1500), "currency": rng.choice(["USD", "USD", "CAD"])},
        "merchant": {"name": rng.choice(["Blue Bottle", "Metro Fuel", "Shopwise", "Cinema 9", "Harbor Hotel"]),
                     "mcc": rng.choice(["5814", "5541", "5411", "7832", "7011"]),
                     "city": CITIES[rng.choice(STATES)], "country": "US"},
        "isFraudFlag": rng.random() < 0.04,
    } for _ in range(n)])
    branches = []
    for i in range(20):
        state = STATES[i % len(STATES)]
        branches.append({
            "branch_id": f"BR{100 + i}", "branch_name": f"{CITIES[state]} {rng.choice(['Main', 'Downtown', 'Harbor', 'North'])}",
            "branch_city": CITIES[state], "branch_state": state,
            "opened_on": day(date(1995, 1, 1), 9000).isoformat(), "manager_name": name(),
        })
    write_csv(SRC / "finance/branch_registry.csv", branches)

    print("wrote 12 sources to", SRC)


if __name__ == "__main__":
    main()
