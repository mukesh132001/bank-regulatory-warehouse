"""
Incremental vs full rebuild of fct_transactions.

Simulates the normal monthly situation: the warehouse already holds every month
up to May, then June's extract arrives.

  1. Load every batch except the last, and build the warehouse (dbt run).
  2. Load the last batch.
  3. Time `dbt run --select fct_transactions` (incremental: only new rows)
     and `dbt run --select fct_transactions --full-refresh` (everything),
     reloading the last batch before each incremental run so every run does
     the same work.
  4. Check that both strategies produce an IDENTICAL table (row count, totals
     and an order-independent fingerprint), then run the full `dbt build`
     so the warehouse ends in its normal, fully tested state.

Results go to benchmarks/results/incremental.md.

Usage:
    python benchmarks/run_incremental_benchmark.py
    python benchmarks/run_incremental_benchmark.py --repeats 5
"""

import argparse
import json
import statistics
import subprocess
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.settings import REPO_ROOT, db_params  # noqa: E402

RESULTS = REPO_ROOT / "benchmarks" / "results" / "incremental.md"
PY = sys.executable

FINGERPRINT_SQL = """
select count(*),
       count(distinct transaction_id),
       sum(amount),
       sum(signed_amount),
       count(customer_version_sk),
       md5(string_agg(transaction_id || ':' || coalesce(customer_version_sk, '') || ':'
                      || amount::text, ',' order by transaction_id))
from warehouse.fct_transactions
"""


def run(cmd, cwd=REPO_ROOT):
    print("  $", " ".join(str(c) for c in cmd))
    subprocess.run(cmd, cwd=cwd, check=True, stdout=subprocess.DEVNULL)


def dbt(*args):
    run([PY, "scripts/dbt.py", *args])


def load(batch):
    run([PY, "loader/load_raw.py", "--batch", batch])


def timed_dbt(*args):
    """Run dbt and return the model's own execution time from run_results.json
    (excludes dbt's start-up time, which is the same for both strategies)."""
    dbt(*args)
    results = json.loads((REPO_ROOT / "dbt_project" / "target" / "run_results.json").read_text())
    return next(r["execution_time"] for r in results["results"]
                if r["unique_id"] == "model.bank_dw.fct_transactions")


def fingerprint(cur):
    cur.execute(FINGERPRINT_SQL)
    return cur.fetchone()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="data/extracts")
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()

    batches = sorted(p.name for p in (REPO_ROOT / args.data).iterdir() if p.is_dir())
    if len(batches) < 2:
        sys.exit("Need at least two batches. Run data_generator/generate_data.py first.")
    earlier, last = batches[:-1], batches[-1]

    conn = psycopg2.connect(**db_params())
    conn.autocommit = True
    cur = conn.cursor()

    print(f"1. Warehouse with {earlier[0]}..{earlier[-1]} only")
    for b in earlier:
        load(b)
    for table in ("customers", "accounts", "transactions", "balances"):
        cur.execute(f"DELETE FROM raw.{table} WHERE _batch_id = %s", (last,))
    cur.execute("DELETE FROM audit.control_totals WHERE batch_id = %s", (last,))
    dbt("run", "--full-refresh")

    print(f"2. {last} arrives")
    incremental_times, full_times = [], []
    for i in range(args.repeats):
        load(last)  # (re)load the new month: gives its rows a fresh _loaded_at
        dbt("run", "--select", "+fct_transactions", "--exclude", "fct_transactions")
        cur.execute("""select count(*) from staging.stg_transactions
                       where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01')
                                           from warehouse.fct_transactions)""")
        new_rows = cur.fetchone()[0]
        incremental_times.append(timed_dbt("run", "--select", "fct_transactions"))
        incremental_fp = fingerprint(cur)
        full_times.append(timed_dbt("run", "--select", "fct_transactions", "--full-refresh"))
        full_fp = fingerprint(cur)
        if incremental_fp != full_fp:
            sys.exit(f"MISMATCH between incremental and full refresh:\n"
                     f"  incremental: {incremental_fp}\n  full:        {full_fp}")
        print(f"   run {i + 1}: incremental {incremental_times[-1]:.2f}s, "
              f"full refresh {full_times[-1]:.2f}s, identical results")

    total_rows = full_fp[0]
    print("3. Restoring the full, tested warehouse (dbt build)")
    dbt("build")
    conn.close()

    inc, full = statistics.median(incremental_times), statistics.median(full_times)
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    RESULTS.write_text(f"""# Incremental vs full rebuild: fct_transactions

New month `{last}` arrives on top of `{earlier[0]}`..`{earlier[-1]}`.
Median of {args.repeats} runs; time = the model's execution time in dbt's run_results.json.

| Strategy | Rows processed | Time |
|---|---|---|
| Full rebuild (`--full-refresh`) | {total_rows:,} | {full:.2f}s |
| Incremental (merge on `transaction_id`) | {new_rows:,} | {inc:.2f}s ({full / inc:.1f}x faster) |

Both strategies produced an identical table on every run: same row count,
totals and fingerprint (md5 over every transaction id, customer version and amount).
""")
    print(f"\nIncremental {inc:.2f}s vs full {full:.2f}s ({full / inc:.1f}x). Written to {RESULTS}")


if __name__ == "__main__":
    main()
