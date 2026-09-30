"""
Raw layer loader: loads monthly extract folders into the `raw` schema.

Guarantees:
  - ACID: each batch loads inside ONE database transaction. If anything fails
    (bad header, row count mismatch, crash), everything rolls back.
  - Idempotent: re-running a batch replaces that batch's rows instead of
    adding them again. Safe to rerun any time.
  - Audited: every row records its batch, source file, line number and load
    time. Every load attempt is logged in audit.load_log.

Usage:
    python loader/load_raw.py                      # load all batches
    python loader/load_raw.py --batch 2026-02      # load one batch
    python loader/load_raw.py --batch 2026-02 --simulate-crash   # test rollback
"""

import argparse
import csv
import json
import sys
import time
from pathlib import Path

import psycopg2

DB = dict(host="localhost", port=5432, dbname="bank_dw",
          user="bank_admin", password="bank_password")

# file name -> (raw table, expected columns in the file)
FILES = {
    "customers.csv": ("raw.customers", [
        "customer_id", "first_name", "last_name", "date_of_birth", "email", "phone",
        "street_address", "city", "state", "postal_code", "risk_rating",
        "customer_since", "last_updated"]),
    "accounts.csv": ("raw.accounts", [
        "account_id", "customer_id", "account_number", "account_type", "currency",
        "open_date", "opening_balance", "opening_balance_date", "status"]),
    "transactions.csv": ("raw.transactions", [
        "transaction_id", "account_id", "transaction_ts", "transaction_type",
        "direction", "amount", "channel", "description"]),
    "balances.csv": ("raw.balances", [
        "account_id", "balance_date", "balance"]),
}


def ddl():
    """SQL to create raw and audit tables if they don't exist yet."""
    stmts = []
    for table, columns in FILES.values():
        cols = ",\n  ".join(f"{c} TEXT" for c in columns)
        name = table.split(".")[1]
        stmts.append(f"""
CREATE TABLE IF NOT EXISTS {table} (
  {cols},
  _batch_id    TEXT        NOT NULL,
  _source_file TEXT        NOT NULL,
  _source_row  INTEGER     NOT NULL,
  _loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_{name}_batch ON {table} (_batch_id);""")

    stmts.append("""
CREATE TABLE IF NOT EXISTS audit.load_log (
  load_id       BIGSERIAL PRIMARY KEY,
  batch_id      TEXT        NOT NULL,
  status        TEXT        NOT NULL,
  rows_loaded   JSONB,
  error_message TEXT,
  started_at    TIMESTAMPTZ NOT NULL,
  finished_at   TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS audit.control_totals (
  batch_id           TEXT    NOT NULL,
  file_name          TEXT    NOT NULL,
  expected_row_count INTEGER NOT NULL,
  expected_total     NUMERIC(18, 2),
  PRIMARY KEY (batch_id, file_name)
);""")
    return "\n".join(stmts)


def check_header(path, columns):
    """Fail if the source system changed the file layout (schema drift)."""
    with open(path, newline="", encoding="utf-8") as f:
        header = next(csv.reader(f))
    if header != columns:
        raise ValueError(f"{path.name}: unexpected columns {header}, expected {columns}")


def log(cur, batch_id, status, summary, message, started):
    cur.execute(
        """INSERT INTO audit.load_log
             (batch_id, status, rows_loaded, error_message, started_at, finished_at)
           VALUES (%s, %s, %s::jsonb, %s, to_timestamp(%s), clock_timestamp())""",
        (batch_id, status, json.dumps(summary), message, started))


def load_batch(conn, batch_dir, simulate_crash=False):
    batch_id = batch_dir.name
    control = json.loads((batch_dir / "control.json").read_text())
    started = time.time()
    summary = {}

    try:
        # `with conn:` = one transaction. Commit if the block finishes,
        # ROLLBACK if any exception happens inside it.
        with conn:
            with conn.cursor() as cur:
                for file_name, (table, columns) in FILES.items():
                    path = batch_dir / file_name
                    check_header(path, columns)
                    col_list = ", ".join(columns)
                    tmp = f"tmp_{table.split('.')[1]}"

                    # Idempotency: remove rows from any earlier load of this batch.
                    # (Table names come from our own FILES dict, not user input,
                    #  so building SQL with f-strings is safe here.)
                    cur.execute(f"DELETE FROM {table} WHERE _batch_id = %s", (batch_id,))

                    # COPY is PostgreSQL's fast bulk loader. We copy into a temp table
                    # whose serial column numbers each row in file order (lineage).
                    cur.execute(f"""CREATE TEMP TABLE {tmp}
                                    (_source_row SERIAL, {", ".join(c + " TEXT" for c in columns)})
                                    ON COMMIT DROP""")
                    with open(path, encoding="utf-8") as f:
                        cur.copy_expert(
                            f"COPY {tmp} ({col_list}) FROM STDIN WITH (FORMAT csv, HEADER true)", f)

                    cur.execute(
                        f"""INSERT INTO {table} ({col_list}, _batch_id, _source_file, _source_row)
                            SELECT {col_list}, %s, %s, _source_row FROM {tmp}""",
                        (batch_id, f"{batch_id}/{file_name}"))
                    loaded = cur.rowcount

                    expected = control["files"][file_name]["row_count"]
                    if loaded != expected:
                        raise ValueError(
                            f"{file_name}: loaded {loaded} rows but control file says {expected}")
                    summary[file_name] = loaded

                    if simulate_crash:
                        raise RuntimeError(f"Simulated crash right after loading {file_name}")

                # Store the control totals for reconciliation later
                cur.execute("DELETE FROM audit.control_totals WHERE batch_id = %s", (batch_id,))
                for fname, info in control["files"].items():
                    cur.execute(
                        """INSERT INTO audit.control_totals
                             (batch_id, file_name, expected_row_count, expected_total)
                           VALUES (%s, %s, %s, %s)""",
                        (batch_id, fname, info["row_count"],
                         info.get("total_amount") or info.get("total_balance")))

                log(cur, batch_id, "SUCCESS", summary, None, started)

    except Exception as e:
        # The batch was rolled back. Record the failure in its own transaction.
        with conn:
            with conn.cursor() as cur:
                log(cur, batch_id, "FAILED", summary, str(e), started)
        raise

    return summary


def main():
    parser = argparse.ArgumentParser(description="Load extracts into the raw layer")
    parser.add_argument("--data", default="data/extracts")
    parser.add_argument("--batch", help="load only this batch, e.g. 2026-02")
    parser.add_argument("--simulate-crash", action="store_true",
                        help="fail after the first file to prove rollback works")
    args = parser.parse_args()

    batches = sorted(p for p in Path(args.data).iterdir() if p.is_dir())
    if args.batch:
        batches = [b for b in batches if b.name == args.batch]
        if not batches:
            sys.exit(f"Batch {args.batch} not found in {args.data}")

    conn = psycopg2.connect(**DB)
    with conn:
        with conn.cursor() as cur:
            cur.execute(ddl())

    for batch_dir in batches:
        t0 = time.time()
        try:
            summary = load_batch(conn, batch_dir, args.simulate_crash)
        except Exception as e:
            print(f"  {batch_dir.name}: FAILED and rolled back -> {e}")
            conn.close()
            sys.exit(1)
        counts = ", ".join(f"{k.replace('.csv', '')}={v:,}" for k, v in summary.items())
        print(f"  {batch_dir.name}: OK in {time.time() - t0:.1f}s ({counts})")

    conn.close()
    print("All batches loaded.")


if __name__ == "__main__":
    main()
