"""
Performance experiment: why does a monthly load get slower as data grows,
and how do indexing and partitioning fix it?

Builds three copies of the same large transactions table in schema `bench`:
  1. txn_plain        - no indexes (the "it got slow" starting point)
  2. txn_indexed      - B-tree indexes on (txn_date) and (account_id, txn_date)
  3. txn_partitioned  - range-partitioned by month, with a local index

Then times three typical monthly operations on each and writes the results
(and EXPLAIN ANALYZE plans) to benchmarks/results/.

Usage:
    python benchmarks/run_benchmark.py                          # 6M rows
    python benchmarks/run_benchmark.py --rows-per-month 200000  # quicker run
"""

import argparse
import statistics
import time
from datetime import date
from pathlib import Path

import psycopg2

DB = dict(host="localhost", port=5432, dbname="bank_dw",
          user="bank_admin", password="bank_password")
RESULTS = Path("benchmarks/results")


def month_starts():
    out, y, m = [], 2025, 7
    for _ in range(12):
        out.append(date(y, m, 1))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


MONTHS = month_starts()   # 2025-07 ... 2026-06


def setup(cur, rows_per_month):
    total = rows_per_month * 12
    print(f"Building test data: {total:,} rows x 3 tables (takes a minute or two)...")

    cur.execute("DROP SCHEMA IF EXISTS bench CASCADE; CREATE SCHEMA bench;")

    # 1. Plain table, no indexes
    cur.execute("""CREATE TABLE bench.txn_plain (
                     txn_id BIGINT, account_id TEXT, txn_date DATE, amount NUMERIC(18,2))""")
    cur.execute(f"""
        INSERT INTO bench.txn_plain
        SELECT g,
               'A' || lpad((1 + floor(random() * 10000))::int::text, 7, '0'),
               date '2025-07-01' + floor(random() * 365)::int,
               round((random() * 1000)::numeric, 2)
        FROM generate_series(1, {total}) AS g""")
    print("  plain table built")

    # 2. Same data, with indexes (created after loading: much faster than before)
    cur.execute("CREATE TABLE bench.txn_indexed AS SELECT * FROM bench.txn_plain")
    cur.execute("CREATE INDEX ix_indexed_date ON bench.txn_indexed (txn_date)")
    cur.execute("CREATE INDEX ix_indexed_acct_date ON bench.txn_indexed (account_id, txn_date)")
    print("  indexed table built")

    # 3. Same data, partitioned by month
    cur.execute("""CREATE TABLE bench.txn_partitioned (
                     txn_id BIGINT, account_id TEXT, txn_date DATE, amount NUMERIC(18,2))
                   PARTITION BY RANGE (txn_date)""")
    for i, start in enumerate(MONTHS):
        end = MONTHS[i + 1] if i + 1 < len(MONTHS) else date(2026, 7, 1)
        cur.execute(f"""CREATE TABLE bench.txn_p_{start:%Y_%m}
                        PARTITION OF bench.txn_partitioned
                        FOR VALUES FROM ('{start}') TO ('{end}')""")
    cur.execute("INSERT INTO bench.txn_partitioned SELECT * FROM bench.txn_plain")
    cur.execute("CREATE INDEX ix_part_acct_date ON bench.txn_partitioned (account_id, txn_date)")
    print("  partitioned table built")

    # The "new" June batch that the monthly reload will insert
    cur.execute("""CREATE TABLE bench.new_month_batch AS
                   SELECT * FROM bench.txn_plain WHERE txn_date >= '2026-06-01'""")

    # Fresh statistics so the query planner makes good decisions
    cur.execute("ANALYZE bench.txn_plain; ANALYZE bench.txn_indexed; "
                "ANALYZE bench.txn_partitioned; ANALYZE bench.new_month_batch;")


def report_sql(table):
    return f"""SELECT account_id, count(*), sum(amount) FROM {table}
               WHERE txn_date >= '2026-06-01' AND txn_date < '2026-07-01'
               GROUP BY account_id"""


def lookup_sql(table):
    return f"""SELECT * FROM {table}
               WHERE account_id = 'A0000042'
                 AND txn_date >= '2026-04-01' AND txn_date < '2026-07-01'"""


def reload_sql(variant, table):
    if variant == "partitioned":
        # Replace one month by emptying its partition: no row-by-row delete
        return """BEGIN;
                  TRUNCATE bench.txn_p_2026_06;
                  INSERT INTO bench.txn_partitioned SELECT * FROM bench.new_month_batch;
                  COMMIT;"""
    return f"""BEGIN;
               DELETE FROM {table} WHERE txn_date >= '2026-06-01' AND txn_date < '2026-07-01';
               INSERT INTO {table} SELECT * FROM bench.new_month_batch;
               COMMIT;"""


def timed(cur, sql, runs):
    """Run a statement several times and return the median duration in seconds."""
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        cur.execute(sql)
        if cur.description:
            cur.fetchall()
        times.append(time.perf_counter() - t0)
    return statistics.median(times)


def fmt(seconds, baseline=None):
    text = f"{seconds:.3f}s"
    if baseline is None:
        return text
    ratio = baseline / seconds
    return f"{text} ({ratio:.1f}x faster)" if ratio >= 1 else f"{text} ({1 / ratio:.1f}x slower)"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows-per-month", type=int, default=500_000)
    args = parser.parse_args()

    RESULTS.mkdir(parents=True, exist_ok=True)
    conn = psycopg2.connect(**DB)
    conn.autocommit = True
    cur = conn.cursor()

    setup(cur, args.rows_per_month)

    variants = {
        "plain": "bench.txn_plain",
        "indexed": "bench.txn_indexed",
        "partitioned": "bench.txn_partitioned",
    }

    # Save query plans: the evidence of WHY each version is fast or slow
    for name, table in variants.items():
        cur.execute("EXPLAIN (ANALYZE, BUFFERS) " + report_sql(table))
        plan = "\n".join(row[0] for row in cur.fetchall())
        (RESULTS / f"explain_report_{name}.txt").write_text(plan + "\n")

    print("\nTiming operations...")
    results = {"Monthly report (aggregate 1 month)": {},
               "Account lookup (1 account, 90 days)": {},
               "Monthly reload (replace 1 month)": {}}
    for name, table in variants.items():
        results["Monthly report (aggregate 1 month)"][name] = timed(cur, report_sql(table), 5)
        results["Account lookup (1 account, 90 days)"][name] = timed(cur, lookup_sql(table), 5)
    for name, table in variants.items():
        results["Monthly reload (replace 1 month)"][name] = timed(cur, reload_sql(name, table), 3)
        print(f"  reload timed: {name}")

    sizes = {}
    for name, table in variants.items():
        if name == "partitioned":
            cur.execute("""SELECT sum(pg_total_relation_size(inhrelid))
                           FROM pg_inherits WHERE inhparent = 'bench.txn_partitioned'::regclass""")
        else:
            cur.execute(f"SELECT pg_total_relation_size('{table}')")
        sizes[name] = cur.fetchone()[0] / 1024 / 1024

    total = args.rows_per_month * 12
    lines = [
        "# Benchmark results",
        "",
        f"{total:,} transactions ({args.rows_per_month:,} per month x 12 months). "
        "Median of 5 runs for queries, 3 runs for reloads.",
        "",
        "| Operation | No index | Indexed | Partitioned |",
        "|---|---|---|---|",
    ]
    for op, r in results.items():
        base = r["plain"]
        lines.append(f"| {op} | {fmt(base)} | {fmt(r['indexed'], base)} | "
                     f"{fmt(r['partitioned'], base)} |")
    lines.append(f"| Disk size incl. indexes | {sizes['plain']:.0f} MB | "
                 f"{sizes['indexed']:.0f} MB | {sizes['partitioned']:.0f} MB |")
    lines += ["", "Query plans: see `explain_report_*.txt` in this folder."]

    report = "\n".join(lines) + "\n"
    (RESULTS / "results.md").write_text(report)
    print("\n" + report)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
