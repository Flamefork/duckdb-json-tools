# JSON Add Prefix Benchmarks

## Current Summary (auto-generated)

<!-- BEGIN GENERATED TABLE -->
| Scenario | Threads | json_add_prefix (ms) | add_prefix_json (ms) | Speedup | json_add_prefix (krows/s) | add_prefix_json (krows/s) |
|---------|--------|---------------------|---------------------|--------|--------------------------|-------------------------|
| arrays_heavy | 1 | 18.56 | 113.00 | 6.09 | 1077.42 | 177.01 |
| arrays_heavy | 8 | 20.79 | 39.04 | 1.88 | 962.02 | 513.25 |
| arrays_heavy_long | 1 | 488.86 | 2954.04 | 6.04 | 1022.90 | 169.26 |
| arrays_heavy_long | 8 | 135.69 | 969.96 | 7.15 | 3684.79 | 515.66 |
| events_nested | 1 | 35.70 | 252.39 | 7.07 | 1400.58 | 198.11 |
| events_nested | 8 | 39.68 | 95.12 | 2.40 | 1259.99 | 526.18 |
| events_nested_long | 1 | 374.93 | 2629.80 | 7.01 | 1333.57 | 190.13 |
| events_nested_long | 8 | 107.17 | 903.14 | 8.43 | 4665.37 | 553.73 |
| events_shallow | 1 | 28.24 | 406.94 | 14.41 | 3541.09 | 245.74 |
| events_shallow | 8 | 32.21 | 164.53 | 5.11 | 3104.24 | 607.84 |
| events_shallow_long | 1 | 319.23 | 4387.96 | 13.75 | 3132.95 | 227.90 |
| events_shallow_long | 8 | 60.28 | 1624.41 | 26.95 | 16590.36 | 615.87 |
| worst_case | 1 | 1.29 | 8.28 | 6.40 | 1545.77 | 241.43 |
| worst_case | 8 | 1.51 | 4.27 | 2.83 | 1328.70 | 468.82 |
<!-- END GENERATED TABLE -->

_Last updated: 2025-10-02T21:24:11_

## How To Reproduce
- Build the release binary: `make release`
- Run the benchmark harness: `python3 scripts/run_json_add_prefix_benchmarks.py --warmup --threads 1 8`
- Refresh this document: `python3 scripts/export_json_add_prefix_results.py`
- Compare against a stored baseline: `python3 scripts/compare_json_add_prefix_results.py --baseline bench/results/json_add_prefix_summary_baseline.csv --candidate bench/results/json_add_prefix_summary.csv --tolerance-pct 2 --sigma-threshold 2`

## Key Findings (manual)
- Fill in with observations after reviewing the summary table.

## Data Artifacts
- `bench/results/json_add_prefix_vs_macro.csv` — full per-run output written by the harness.
- `bench/results/json_add_prefix_summary.csv` — aggregated averages exported by `export_json_add_prefix_results.py`.
- `bench/json_add_prefix_bench.duckdb` — DuckDB database containing the `bench_results` table with raw metrics.
- `build/bench/profiles/json_add_prefix/threads_<threads>/<scenario>/` — profiling JSONs for each run.
