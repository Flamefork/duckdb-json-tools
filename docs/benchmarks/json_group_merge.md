# JSON Group Merge Benchmarks

## Current Summary (auto-generated)

<!-- BEGIN GENERATED TABLE -->
| Scenario | Threads | json_group_merge (ms) | list_reduce (ms) | Speedup | json_group_merge (krows/s) | list_reduce (krows/s) |
|---------|--------|----------------------|-----------------|--------|---------------------------|---------------------|
| arrays_replace | 1 | 145.65 | 293.34 | 2.01 | 1098.50 | 545.96 |
| arrays_replace | 8 | 153.70 | 289.13 | 1.88 | 1041.51 | 553.42 |
| customers_nested | 1 | 246.81 | 517.87 | 2.10 | 972.43 | 463.45 |
| customers_nested | 8 | 115.79 | 180.10 | 1.56 | 2073.39 | 1335.58 |
| large_docs | 1 | 58.62 | 93.10 | 1.59 | 545.91 | 343.72 |
| large_docs | 8 | 60.09 | 96.13 | 1.60 | 532.54 | 332.89 |
| scalars_mix | 1 | 121.49 | 190.88 | 1.57 | 1234.72 | 785.88 |
| scalars_mix | 8 | 74.44 | 110.61 | 1.49 | 2015.23 | 1385.73 |
| sessions_heavy_docs | 1 | 9970.97 | 23052.76 | 2.31 | 4.11 | 1.78 |
| sessions_heavy_docs | 8 | 10354.53 | 24387.02 | 2.36 | 3.96 | 1.68 |
| sessions_shallow_asc | 1 | 64.20 | 103.88 | 1.62 | 1557.53 | 962.75 |
| sessions_shallow_asc | 8 | 66.60 | 105.78 | 1.59 | 1501.62 | 945.38 |
| sessions_shallow_desc | 1 | 70.04 | 110.46 | 1.58 | 1428.15 | 905.35 |
| sessions_shallow_desc | 8 | 68.58 | 110.95 | 1.62 | 1458.16 | 901.40 |
| sessions_shallow_long | 1 | 514.78 | 1137.36 | 2.21 | 1942.60 | 879.23 |
| sessions_shallow_long | 8 | 234.09 | 356.96 | 1.52 | 4272.93 | 2802.32 |
<!-- END GENERATED TABLE -->

_Last updated: 2025-10-14T15:21:55_

## How To Reproduce
- Build the release binary: `make release`
- Run the benchmark harness: `python3 scripts/run_json_group_merge_benchmarks.py --warmup --threads 1 8`
- Refresh this document: `python3 scripts/export_json_group_merge_results.py`
- Compare against a stored baseline: `python3 scripts/compare_json_group_merge_results.py --baseline bench/results/json_group_merge_summary_baseline.csv --candidate bench/results/json_group_merge_summary.csv --tolerance-pct 2 --sigma-threshold 2`

## Scenario Notes
- **sessions_heavy_docs** — 40,960 deterministic patches (512 groups × 80 events) with a null sentinel every 48 events; each JSON payload carries 160 top-level keys, nested depth 4 objects/arrays, and averages ~92 KB serialized size (91–92 KB range) to stress allocator growth while exercising overwrite and compaction paths.

## Key Findings (manual)
- Fill in with observations after reviewing the summary table.

## Data Artifacts
- `bench/results/json_group_merge_vs_list_reduce.csv` — full per-run output written by the harness.
- `bench/results/json_group_merge_summary.csv` — aggregated averages exported by `export_json_group_merge_results.py`.
- `bench/json_group_merge_bench.duckdb` — DuckDB database containing the `bench_results` table with raw metrics.
- `build/bench/profiles/json_group_merge/threads_<threads>/<scenario>/` — profiling JSONs for each run.
