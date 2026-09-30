# INCIDENT-001: Warehouse balances did not match official balances

## Symptom
The reconciliation test `assert_reconciliation_passes` failed. Month-end balances
computed by the warehouse did not match the core banking system's official balances.

| Month | Accounts checked | Breaks | Total absolute difference |
|---|---|---|---|
| 2026-01 | 6,487 | 96 | 45,842.82 |
| 2026-02 | 6,585 | 176 | 83,416.62 |
| 2026-03 | 6,684 | 256 | 119,906.24 |
| 2026-04 | 6,803 | 332 | 147,867.13 |
| 2026-05 | 6,918 | 414 | 186,859.24 |
| 2026-06 | 7,041 | 414 | 186,859.24 |

## Investigation
1. **File controls all passed.** Every file arrived complete, so the source data
   was not the problem. The issue was inside the pipeline.
2. **Transaction count check failed:** the warehouse had 432 more rows than
   unique source transactions, meaning something was counted twice.
3. **Breaks grew each month, then stopped.** Each batch re-sends some of the
   previous month's transactions, so errors accumulated. There is no July
   batch to re-send June transactions, so May and June totals were equal.
4. **Account A0002047 was off by exactly 5,810.69 every month.** A constant
   offset is the signature of a single transaction counted twice.
5. **Lineage columns revealed the cause:** duplicates had the same
   `transaction_id` but different `_batch_id` values (e.g. 2026-01 and 2026-02).

## Root cause
`stg_transactions` deduplicated with `partition by transaction_id, _batch_id`.
This only removed duplicates within the same file. When the source system
re-sent a transaction in a later month's file, both copies survived.

## Fix
Deduplicate on `transaction_id` alone across all batches, keeping the first
copy received (`order by _batch_id, _source_row`), so lineage points to the
original delivery.

## Prevention
- Added a `unique` test on `stg_transactions.transaction_id`, so duplicates
  now fail the pipeline at the staging layer, before they reach any report.
- The reconciliation test fails the pipeline if any check fails, so
  unreconciled numbers cannot reach a regulatory report.

## Lessons
- Matching row counts at file level does not prove the numbers are correct.
  Reconcile at several levels: files, records, and balances.
- Audit and lineage columns (`_batch_id`, `_source_row`) turned a vague
  "balances are off" into an exact root cause within minutes.
