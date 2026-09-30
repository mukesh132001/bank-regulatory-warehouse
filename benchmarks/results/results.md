# Benchmark results

6,000,000 transactions (500,000 per month x 12 months). Median of 5 runs for queries, 3 runs for reloads.

| Operation | No index | Indexed | Partitioned |
|---|---|---|---|
| Monthly report (aggregate 1 month) | 0.366s | 0.507s (1.4x slower) | 0.208s (1.8x faster) |
| Account lookup (1 account, 90 days) | 0.255s | 0.001s (201.7x faster) | 0.001s (243.2x faster) |
| Monthly reload (replace 1 month) | 3.396s | 10.888s (3.2x slower) | 3.149s (1.1x faster) |
| Disk size incl. indexes | 430 MB | 666 MB | 476 MB |

Query plans: see `explain_report_*.txt` in this folder.
