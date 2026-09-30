"""
Bank data generator.

Simulates monthly extracts from a core banking system (Jan-Jun 2026).
Each month gets a folder with customers, accounts, transactions,
official month-end balances, and a control file with counts and totals.

Dirty data is added ON PURPOSE so the warehouse has real problems to solve:
  - duplicate transaction rows (within a file, and re-sent from last month)
  - late-arriving transactions (last month's activity in this month's file)
  - messy customer fields (extra spaces, inconsistent casing, missing emails)
  - customer changes over time (address moves, risk rating, email)
  - dormant customers (no customer activity in the last 90 days)

Usage:
    python data_generator/generate_data.py
    python data_generator/generate_data.py --customers 50000   # bigger, for benchmarks
"""

import argparse
import csv
import itertools
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from faker import Faker

SEED = 42
START_DATE = date(2026, 1, 1)
NUM_MONTHS = 6
DORMANT_CUTOFF = date(2026, 3, 25)  # dormant customers stop before this date
RISK = ["LOW", "MEDIUM", "HIGH"]

fake = Faker("en_US")

# (type, direction, min_cents, max_cents, channel, weight)
CHECKING_TYPES = [
    ("CARD_PURCHASE", "DR", 500, 25000, "CARD", 55),
    ("ATM_WITHDRAWAL", "DR", 2000, 40000, "ATM", 10),
    ("TRANSFER_OUT", "DR", 5000, 150000, "ONLINE", 10),
    ("BILL_PAYMENT", "DR", 3000, 50000, "ONLINE", 12),
    ("DEPOSIT", "CR", 2000, 100000, "BRANCH", 8),
    ("TRANSFER_IN", "CR", 5000, 150000, "ONLINE", 5),
]
SAVINGS_TYPES = [
    ("DEPOSIT", "CR", 5000, 200000, "BRANCH", 40),
    ("TRANSFER_IN", "CR", 5000, 100000, "ONLINE", 35),
    ("WITHDRAWAL", "DR", 5000, 100000, "BRANCH", 25),
]

CUSTOMER_FIELDS = ["customer_id", "first_name", "last_name", "date_of_birth", "email",
                   "phone", "street_address", "city", "state", "postal_code",
                   "risk_rating", "customer_since", "last_updated"]
ACCOUNT_FIELDS = ["account_id", "customer_id", "account_number", "account_type", "currency",
                  "open_date", "opening_balance", "opening_balance_date", "status"]
TXN_FIELDS = ["transaction_id", "account_id", "transaction_ts", "transaction_type",
              "direction", "amount", "channel", "description"]
BALANCE_FIELDS = ["account_id", "balance_date", "balance"]


def month_bounds(i):
    """Return (first_day, last_day) of month i, where 0 = January 2026."""
    first = date(START_DATE.year, START_DATE.month + i, 1)
    nxt = date(first.year + 1, 1, 1) if first.month == 12 else date(first.year, first.month + 1, 1)
    return first, nxt - timedelta(days=1)


END_DATE = month_bounds(NUM_MONTHS - 1)[1]


def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))


def money(cents):
    """Format integer cents as a decimal string. We use integer cents
    internally so there are no floating-point rounding errors."""
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    return f"{sign}{cents // 100}.{cents % 100:02d}"


def write_csv(path, rows, fields):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def make_customer(n, customer_since):
    first, last = fake.first_name(), fake.last_name()
    email = f"{first}.{last}{random.randint(1, 99)}@{fake.free_email_domain()}".lower()
    state = fake.state_abbr()

    # Dirty data: the source system does not validate input well
    if random.random() < 0.05:
        first = f"  {first} "          # extra spaces
    if random.random() < 0.10:
        email = email.upper()          # inconsistent casing
    if random.random() < 0.04:
        email = ""                     # missing email
    if random.random() < 0.05:
        state = state.lower()          # inconsistent casing

    return {
        "customer_id": f"C{n:06d}",
        "first_name": first,
        "last_name": last,
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=85).isoformat(),
        "email": email,
        "phone": fake.phone_number(),
        "street_address": fake.street_address(),
        "city": fake.city(),
        "state": state,
        "postal_code": fake.postcode(),
        "risk_rating": random.choices(RISK, weights=[70, 25, 5])[0],
        "customer_since": customer_since.isoformat(),
        "last_updated": f"{customer_since.isoformat()} 09:00:00",
    }


def make_account(n, customer_id, account_type, open_date, balance_date):
    low, high = (10000, 800000) if account_type == "CHECKING" else (50000, 3000000)
    return {
        "account_id": f"A{n:07d}",
        "customer_id": customer_id,
        "account_number": str(random.randint(10**9, 10**10 - 1)),
        "account_type": account_type,
        "currency": "USD",
        "open_date": open_date.isoformat(),
        "opening_balance": random.randint(low, high),   # cents
        "opening_balance_date": balance_date.isoformat(),
        "status": "ACTIVE",
    }


def generate_transactions(account, profile, merchants):
    """Create one account's transactions day by day with a running balance.
    Returns the transactions and the true balance at each month end."""
    is_checking = account["account_type"] == "CHECKING"
    types = CHECKING_TYPES if is_checking else SAVINGS_TYPES
    weights = [t[5] for t in types]
    daily_rate = (20 if is_checking else 3) / 30 * profile["activity"]
    salary_day = random.randint(1, 28)

    balance = account["opening_balance"]
    day = date.fromisoformat(account["opening_balance_date"])
    txns, month_end_balances = [], {}

    while day <= END_DATE:
        is_month_end = (day + timedelta(days=1)).day == 1
        todays = []

        if day <= profile["last_active"]:
            if is_checking and day.day == salary_day:
                todays.append((random.randint(0, 86399), "SALARY", "CR",
                               random.randint(150000, 600000), "ACH", "Payroll deposit"))
            count = int(daily_rate) + (1 if random.random() < daily_rate % 1 else 0)
            for _ in range(count):
                ttype, direction, low, high, channel, _w = random.choices(types, weights=weights)[0]
                desc = random.choice(merchants) if ttype == "CARD_PURCHASE" else ttype.replace("_", " ").title()
                todays.append((random.randint(0, 86399), ttype, direction,
                               random.randint(low, high), channel, desc))

        todays.sort()

        # The SYSTEM posts interest at month end, even for inactive customers.
        # This matters later: "dormant" must ignore system-generated transactions.
        if not is_checking and is_month_end:
            todays.append((86340, "INTEREST", "CR", None, "SYSTEM", "Monthly interest"))

        for secs, ttype, direction, amount, channel, desc in todays:
            if ttype == "INTEREST":
                amount = balance * 4 // 1200        # 4% a year, paid monthly
                if amount <= 0:
                    continue
            if direction == "DR" and amount > balance:
                continue                            # declined: insufficient funds
            balance += amount if direction == "CR" else -amount
            ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=secs)
            txns.append({
                "account_id": account["account_id"],
                "transaction_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "transaction_type": ttype,
                "direction": direction,
                "amount": amount,
                "channel": channel,
                "description": desc,
            })

        if is_month_end:
            month_end_balances[day] = balance
        day += timedelta(days=1)

    return txns, month_end_balances


def main():
    parser = argparse.ArgumentParser(description="Generate fake banking extracts")
    parser.add_argument("--customers", type=int, default=5000)
    parser.add_argument("--out", default="data/extracts")
    args = parser.parse_args()

    random.seed(SEED)
    Faker.seed(SEED)
    merchants = [fake.company() for _ in range(300)]
    stats = dict.fromkeys(["dormant", "address_changes", "risk_changes", "email_changes",
                           "late_arriving", "dup_within_file", "dup_resent"], 0)

    # 1. Customers (10% join during the period, 20% become dormant)
    print("Generating customers...")
    customers, profiles = [], {}
    for n in range(1, args.customers + 1):
        if random.random() < 0.10:
            since = random_date(START_DATE, END_DATE)
        else:
            since = random_date(date(2010, 1, 1), START_DATE - timedelta(days=1))
        c = make_customer(n, since)
        customers.append(c)
        if random.random() < 0.20:
            last_active = random_date(START_DATE - timedelta(days=1), DORMANT_CUTOFF)
            stats["dormant"] += 1
        else:
            last_active = END_DATE
        profiles[c["customer_id"]] = {"activity": random.uniform(0.5, 1.5),
                                      "last_active": last_active}

    # 2. Accounts (most customers have checking, some also savings)
    print("Generating accounts...")
    accounts = []
    for c in customers:
        since = date.fromisoformat(c["customer_since"])
        types = [t for t, p in (("CHECKING", 0.9), ("SAVINGS", 0.45)) if random.random() < p] or ["CHECKING"]
        for t in types:
            if since >= START_DATE:
                open_d, bal_d = since, since
            else:
                open_d, bal_d = random_date(since, START_DATE - timedelta(days=1)), START_DATE
            accounts.append(make_account(len(accounts) + 1, c["customer_id"], t, open_d, bal_d))

    # 3. Transactions
    print("Generating transactions (this takes a little while)...")
    all_txns = []
    for a in accounts:
        txns, meb = generate_transactions(a, profiles[a["customer_id"]], merchants)
        a["month_end_balances"] = meb
        all_txns.extend(txns)
    all_txns.sort(key=lambda t: (t["transaction_ts"], t["account_id"]))
    for i, t in enumerate(all_txns, start=1):
        t["transaction_id"] = f"T{i:09d}"

    # 4. Assign transactions to monthly batches (1% arrive a month late)
    batches = [[] for _ in range(NUM_MONTHS)]
    for t in all_txns:
        m = int(t["transaction_ts"][5:7]) - START_DATE.month
        if m < NUM_MONTHS - 1 and random.random() < 0.01:
            m += 1
            stats["late_arriving"] += 1
        batches[m].append(t)

    # 5. Write one folder per month
    out = Path(args.out)
    for m in range(NUM_MONTHS):
        first, last = month_bounds(m)
        batch_id = first.strftime("%Y-%m")
        bdir = out / batch_id
        bdir.mkdir(parents=True, exist_ok=True)

        # Customer changes during this month (existing customers only)
        if m > 0:
            for c in customers:
                if date.fromisoformat(c["customer_since"]) >= first:
                    continue
                changed = False
                if random.random() < 0.03:     # moved house -> SCD Type 2
                    c["street_address"], c["city"] = fake.street_address(), fake.city()
                    c["state"], c["postal_code"] = fake.state_abbr(), fake.postcode()
                    changed = True
                    stats["address_changes"] += 1
                if random.random() < 0.02:     # risk review -> SCD Type 2 / 3
                    c["risk_rating"] = random.choice([r for r in RISK if r != c["risk_rating"]])
                    changed = True
                    stats["risk_changes"] += 1
                if random.random() < 0.01:     # new email -> SCD Type 1
                    c["email"] = (f"{c['first_name'].strip()}.{c['last_name']}"
                                  f"{random.randint(100, 999)}@{fake.free_email_domain()}").lower()
                    changed = True
                    stats["email_changes"] += 1
                if changed:
                    c["last_updated"] = (f"{random_date(first, last).isoformat()} "
                                         f"{random.randint(8, 18):02d}:{random.randint(0, 59):02d}:00")

        cust_rows = [c for c in customers if date.fromisoformat(c["customer_since"]) <= last]
        acc_open = [a for a in accounts if date.fromisoformat(a["open_date"]) <= last]
        acc_rows = [{**a, "opening_balance": money(a["opening_balance"])} for a in acc_open]

        txn_rows = list(batches[m])
        dup_within = random.sample(txn_rows, int(len(txn_rows) * 0.003))
        dup_resent = random.sample(batches[m - 1], int(len(batches[m - 1]) * 0.001)) if m > 0 else []
        stats["dup_within_file"] += len(dup_within)
        stats["dup_resent"] += len(dup_resent)
        txn_rows = txn_rows + dup_within + dup_resent
        txn_rows.sort(key=lambda t: t["transaction_ts"])
        txn_total = sum(t["amount"] for t in txn_rows)
        txn_out = [{**t, "amount": money(t["amount"])} for t in txn_rows]

        bal_cents = [(a["account_id"], a["month_end_balances"][last]) for a in acc_open]
        bal_rows = [{"account_id": aid, "balance_date": last.isoformat(), "balance": money(b)}
                    for aid, b in bal_cents]

        write_csv(bdir / "customers.csv", cust_rows, CUSTOMER_FIELDS)
        write_csv(bdir / "accounts.csv", acc_rows, ACCOUNT_FIELDS)
        write_csv(bdir / "transactions.csv", txn_out, TXN_FIELDS)
        write_csv(bdir / "balances.csv", bal_rows, BALANCE_FIELDS)

        control = {
            "batch_id": batch_id,
            "extract_date": (last + timedelta(days=1)).isoformat(),
            "files": {
                "customers.csv": {"row_count": len(cust_rows)},
                "accounts.csv": {"row_count": len(acc_rows)},
                "transactions.csv": {"row_count": len(txn_rows), "total_amount": money(txn_total)},
                "balances.csv": {"row_count": len(bal_rows),
                                 "total_balance": money(sum(b for _, b in bal_cents))},
            },
        }
        (bdir / "control.json").write_text(json.dumps(control, indent=2))
        print(f"  {batch_id}: {len(cust_rows):,} customers, {len(acc_rows):,} accounts, "
              f"{len(txn_rows):,} transactions")

    print("\nDone. Summary:")
    print(f"  Unique transactions generated: {len(all_txns):,}")
    for k, v in stats.items():
        print(f"  {k}: {v:,}")


if __name__ == "__main__":
    main()
