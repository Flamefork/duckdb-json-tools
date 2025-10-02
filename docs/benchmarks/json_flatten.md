# JSON Flatten Benchmarks

## Current Summary (auto-generated)

<!-- BEGIN GENERATED TABLE -->
| Scenario | Threads | json_flatten (ms) | flatten_json (ms) | Speedup | json_flatten (krows/s) | flatten_json (krows/s) |
|---------|--------|------------------|------------------|--------|-----------------------|----------------------|
| arrays_heavy | 1 | 42.40 | 659.82 | 15.56 | 471.74 | 30.31 |
| arrays_heavy | 8 | 46.14 | 136.83 | 2.97 | 433.48 | 146.22 |
| arrays_heavy_long | 1 | 1076.10 | 17740.24 | 16.49 | 464.64 | 28.18 |
| arrays_heavy_long | 8 | 358.72 | 3300.72 | 9.20 | 1393.88 | 151.80 |
| events_nested | 1 | 68.92 | 994.81 | 14.43 | 725.43 | 50.26 |
| events_nested | 8 | 74.37 | 205.03 | 2.76 | 672.35 | 244.49 |
| events_nested_long | 1 | 730.32 | 11321.76 | 15.50 | 684.64 | 44.16 |
| events_nested_long | 8 | 263.22 | 2222.58 | 8.44 | 1899.57 | 224.97 |
| events_shallow | 1 | 49.01 | 516.24 | 10.53 | 2040.43 | 193.71 |
| events_shallow | 8 | 54.04 | 158.97 | 2.94 | 1850.53 | 631.22 |
| events_shallow_long | 1 | 532.63 | 6304.83 | 11.84 | 1877.63 | 158.61 |
| events_shallow_long | 8 | 190.06 | 1697.39 | 8.93 | 5275.15 | 589.27 |
| worst_case | 1 | 3.10 | 39.46 | 12.73 | 655.11 | 50.70 |
| worst_case | 8 | 3.19 | 9.82 | 3.08 | 627.30 | 203.80 |
<!-- END GENERATED TABLE -->

_Last updated: 2025-09-30T19:04:53_

## How To Reproduce
- Build the release binary: `make release`
- Run the benchmark harness: `python3 scripts/run_json_flatten_benchmarks.py --warmup --threads 1 8`
- Refresh this document: `python3 scripts/export_json_flatten_results.py`
- Compare against a stored baseline: `python3 scripts/compare_json_flatten_results.py --baseline bench/results/json_flatten_summary_baseline.csv --candidate bench/results/json_flatten_summary.csv --tolerance-pct 2 --sigma-threshold 2`

## Key Findings (manual)
- Fill in with observations after reviewing the summary table.

## Data Artifacts
- `bench/results/json_flatten_vs_macro.csv` — full per-run output written by the harness.
- `bench/results/json_flatten_summary.csv` — aggregated averages exported by `export_json_flatten_results.py`.
- `bench/json_flatten_bench.duckdb` — DuckDB database containing the `bench_results` table with raw metrics.
- `build/bench/profiles/threads_<threads>/<scenario>/` — profiling JSONs for each run.
