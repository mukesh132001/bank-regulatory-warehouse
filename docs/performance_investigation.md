# Performance Investigation: "The monthly load went from 2 hours to 8"

## How to diagnose before fixing

1. **Find which step got slower.** Compare timings per step (load, transform,
   tests) over time instead of guessing. Our `audit.load_log` records start and
   finish times for every load.
2. **Check data growth.** A query that scans the whole table gets slower every
   month even if nothing else changes.
3. **Read the query plan** with `EXPLAIN (ANALYZE, BUFFERS)`: look for
   `Seq Scan` on big tables, large `Rows Removed by Filter`, and high buffer reads.
4. **Check table health** in `pg_stat_user_tables`: many sequential scans,
   many dead rows (`n_dead_tup`) from deletes, and when `ANALYZE` last ran.
5. **Check for recently added indexes**, which speed up reads but slow down writes.
6. **Check locks and concurrent jobs** in `pg_stat_activity`.

## Experiment

Three copies of the same 6,000,000-row transactions table (500,000 rows per
month x 12 months), timed on three operations that mirror this project.
Script: `benchmarks/run_benchmark.py`. Raw output: `benchmarks/results/`.

| Operation | No index | Indexed | Partitioned by month |
|---|---|---|---|
| Monthly report (aggregate 1 month) | 0.366s | 0.507s (1.4x slower) | 0.208s (1.8x faster) |
| Account lookup (1 account, 90 days) | 0.255s | 0.001s (201.7x faster) | 0.001s (243.2x faster) |
| Monthly reload (replace 1 month) | 3.396s | 10.888s (3.2x slower) | 3.149s (1.1x faster) |
| Disk size incl. indexes | 430 MB | 666 MB | 476 MB |

## Findings

**1. Partition pruning cuts wasted reading.** From the query plans for the
monthly report:

| | Plain table | Partitioned |
|---|---|---|
| Table scanned | `txn_plain` (all 12 months) | `txn_p_2026_06` (June only) |
| Rows read and discarded | ~5.5 million | 0 |
| Pages read (8 KB each) | 44,118 (~345 MB) | 3,626 (~28 MB) |

Data read dropped 12x but time only ~2x, because both still aggregate the same
500,000 June rows. The gap grows with history: with 5 years of data the plain
table would scan 60 months to use 1.

**2. Indexes help selective queries, not large ones.** The account lookup was
200x+ faster with an index. The monthly report, which reads ~8% of the table,
was *slower* with an index, because sequential reading beats index jumps when
a query needs a large share of the rows.

**3. Indexes slow down writes.** The indexed reload was 3.2x slower because
every deleted and inserted row also updates two indexes. Adding indexes to
speed up reports can silently make the load job much slower: a realistic cause
of "2 hours became 8."

**4. Partitioning gives the best of both.** Same 1 ms lookup as the indexed
table, a 3.5x faster reload than the indexed table (3.1s vs 10.9s), faster
reports, and less disk. `TRUNCATE` of one partition also leaves no dead rows,
unlike `DELETE`, which creates bloat until `VACUUM` runs.

## Recommendation for this warehouse

- Partition large transaction tables (raw and fact) by month.
- Index only for selective access patterns, e.g. `(account_id, txn_date)`.
  Avoid indexes that only serve large scans.
- Make the idempotent monthly reload replace a whole partition
  (`TRUNCATE`, or load a staging table and `ATTACH PARTITION`) instead of
  row-by-row `DELETE`.
- Run `ANALYZE` after each load so the planner has fresh statistics.
