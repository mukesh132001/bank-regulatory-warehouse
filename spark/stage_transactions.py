"""
PySpark version of the transactions staging step (dbt model: stg_transactions).

What it does, in the same order a production Spark job would:
  1. Reads every monthly transactions extract with an EXPLICIT schema
     (all text, exactly like the raw layer; nothing is inferred).
  2. Adds lineage: batch id, source file and source row number.
  3. Types every column with try_* casts, and QUARANTINES rows that fail
     validation (bad timestamp, bad amount, unknown direction, missing id)
     instead of crashing or silently dropping them.
  4. Deduplicates ACROSS batches with a window function:
     row_number() over (partition by transaction_id order by batch, source row)
     keeps the first copy received (the INCIDENT-001 fix, now in Spark).
  5. Writes partitioned Parquet (by transaction month) plus the quarantine set.
  6. Optional parity check (--check-against-postgres): compares the Spark result
     with dbt's staging.stg_transactions and exits non-zero on any mismatch.

Usage:
    python spark/stage_transactions.py
    python spark/stage_transactions.py --check-against-postgres
"""

import argparse
import json
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

RAW_COLUMNS = ["transaction_id", "account_id", "transaction_ts", "transaction_type",
               "direction", "amount", "channel", "description"]
RAW_SCHEMA = T.StructType([T.StructField(c, T.StringType(), nullable=True) for c in RAW_COLUMNS])


def spark_session(master: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName("bank_dw_stage_transactions")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        # Keep every extract file in one split so row order (lineage) is preserved.
        .config("spark.sql.files.maxPartitionBytes", str(1 << 30))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw(spark: SparkSession, input_dir: str):
    raw = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(RAW_SCHEMA)
        .csv(f"{input_dir}/*/transactions.csv")
        .withColumn("_file_path", F.col("_metadata.file_path"))
        .withColumn("_read_order", F.monotonically_increasing_id())
    )
    batch_id = F.regexp_extract("_file_path", r"(\d{4}-\d{2})/transactions\.csv$", 1)
    in_file_order = Window.partitionBy("_file_path").orderBy("_read_order")
    return (
        raw.withColumn("_batch_id", batch_id)
        .withColumn("_source_file", F.concat(batch_id, F.lit("/transactions.csv")))
        .withColumn("_source_row", F.row_number().over(in_file_order))
        .drop("_file_path", "_read_order")
    )


def type_and_validate(raw):
    typed = raw.select(
        "transaction_id",
        "account_id",
        F.expr("try_to_timestamp(transaction_ts, 'yyyy-MM-dd HH:mm:ss')").alias("transaction_ts"),
        "transaction_type",
        "direction",
        F.expr("try_cast(amount AS DECIMAL(18,2))").alias("amount"),
        "channel",
        "description",
        "_batch_id",
        "_source_file",
        "_source_row",
    )
    reason = (
        F.when(F.col("transaction_id").isNull(), "missing transaction_id")
        .when(F.col("transaction_ts").isNull(), "bad transaction_ts")
        .when(F.col("amount").isNull(), "bad amount")
        .when(F.col("direction").isNull() | ~F.col("direction").isin("CR", "DR"), "bad direction")
    )
    typed = typed.withColumn("_reject_reason", reason)
    valid = typed.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")
    quarantined = typed.filter(F.col("_reject_reason").isNotNull())
    return valid, quarantined


def deduplicate(valid):
    first_copy = Window.partitionBy("transaction_id").orderBy("_batch_id", "_source_row")
    return (
        valid.withColumn("copy_number", F.row_number().over(first_copy))
        .filter(F.col("copy_number") == 1)
        .drop("copy_number")
        .withColumn("transaction_date", F.to_date("transaction_ts"))
        .withColumn("signed_amount",
                    F.when(F.col("direction") == "CR", F.col("amount")).otherwise(-F.col("amount")))
        .withColumn("is_customer_initiated", F.col("channel") != F.lit("SYSTEM"))
        .withColumn("transaction_month", F.date_format("transaction_date", "yyyy-MM"))
    )


def spark_metrics(staged, quarantined) -> dict:
    totals = staged.agg(
        F.count("*").alias("rows"),
        F.countDistinct("transaction_id").alias("distinct_ids"),
        F.sum("amount").alias("total_amount"),
        F.sum("signed_amount").alias("total_signed_amount"),
    ).first()
    per_batch = {r["_batch_id"]: r["n"] for r in
                 staged.groupBy("_batch_id").agg(F.count("*").alias("n")).collect()}
    return {
        "rows": totals["rows"],
        "distinct_ids": totals["distinct_ids"],
        "total_amount": str(totals["total_amount"]),
        "total_signed_amount": str(totals["total_signed_amount"]),
        "rows_per_batch": dict(sorted(per_batch.items())),
        "quarantined_rows": quarantined.count(),
    }


def postgres_metrics() -> dict:
    import psycopg2

    from common.settings import db_params

    with psycopg2.connect(**db_params()) as conn, conn.cursor() as cur:
        cur.execute("""select count(*), count(distinct transaction_id),
                              sum(amount), sum(signed_amount)
                       from staging.stg_transactions""")
        rows, distinct_ids, total, signed = cur.fetchone()
        cur.execute("""select _batch_id, count(*) from staging.stg_transactions
                       group by 1 order by 1""")
        per_batch = dict(cur.fetchall())
    return {
        "rows": rows,
        "distinct_ids": distinct_ids,
        "total_amount": str(total),
        "total_signed_amount": str(signed),
        "rows_per_batch": per_batch,
    }


def main():
    parser = argparse.ArgumentParser(description="PySpark staging for transactions")
    parser.add_argument("--input", default="data/extracts", help="folder with <batch>/transactions.csv")
    parser.add_argument("--output", default="data/spark", help="where Parquet output is written")
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--check-against-postgres", action="store_true",
                        help="compare with dbt's staging.stg_transactions; exit 1 on mismatch")
    args = parser.parse_args()

    t0 = time.time()
    spark = spark_session(args.master)
    out = Path(args.output)

    valid, quarantined = type_and_validate(read_raw(spark, args.input))
    staged = deduplicate(valid).cache()

    (staged.write.mode("overwrite").partitionBy("transaction_month")
     .parquet(str(out / "staging" / "transactions")))
    quarantined.write.mode("overwrite").parquet(str(out / "quarantine" / "transactions"))

    report = {"spark": spark_metrics(staged, quarantined)}
    print(f"Spark staging: {report['spark']['rows']:,} transactions, "
          f"{report['spark']['quarantined_rows']:,} quarantined, {time.time() - t0:.1f}s")

    exit_code = 0
    if args.check_against_postgres:
        report["dbt"] = postgres_metrics()
        checks = ["rows", "distinct_ids", "total_amount", "total_signed_amount", "rows_per_batch"]
        report["mismatches"] = [c for c in checks if report["spark"][c] != report["dbt"][c]]
        print(f"\n{'check':<22}{'spark':>22}{'dbt':>22}")
        for c in checks[:-1]:
            print(f"{c:<22}{report['spark'][c]!s:>22}{report['dbt'][c]!s:>22}")
        same_batches = "same" if "rows_per_batch" not in report["mismatches"] else "DIFFERENT"
        print(f"{'rows_per_batch':<22}{same_batches:>44}")
        if report["mismatches"]:
            print(f"\nPARITY FAILED: {', '.join(report['mismatches'])}")
            exit_code = 1
        else:
            print("\nPARITY PASSED: Spark and dbt staging agree exactly.")

    out.mkdir(parents=True, exist_ok=True)
    (out / "parity_report.json").write_text(json.dumps(report, indent=2))
    spark.stop()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
