# Benchmark Suite

Performance benchmarks for json_tools DuckDB extension.

## Quickstart

```bash
# Build the extension first
make release

# Run benchmarks (generates data if needed, runs sanity checks, compares with baseline)
uv run python bench/bench.py

# Save current results as new baseline (after accepting performance changes)
uv run python bench/compare_results.py --save-baseline
```

## Filtering Benchmarks

Use `--filter` with substring matching to run specific benchmarks:

```bash
# All benchmarks for a function
uv run python bench/run_benchmarks.py --filter json_group_merge

# All benchmarks for a size
uv run python bench/run_benchmarks.py --filter 10k

# All benchmarks for a scenario
uv run python bench/run_benchmarks.py --filter few_groups

# Specific case
uv run python bench/run_benchmarks.py --filter json_group_merge/10k/few_groups
```

Case ID format: `function/size/scenario` (e.g., `json_flatten/10k/basic`).

## Artifacts

All artifacts are in `bench/results/`:

| File | Description |
|------|-------------|
| `latest.json` | Most recent benchmark run |
| `baseline.json` | Committed baseline for comparison |
| `diff.json` | Comparison between latest and baseline |
| `profiles/<case>/` | DuckDB query profiles (when collected) |

## Interpreting Results

### Statuses

| Status | Meaning |
|--------|---------|
| `SLOWER` | Regression: current is slower than baseline beyond tolerance |
| `FASTER` | Improvement: current is faster than baseline beyond tolerance |
| `UNCHANGED` | Within tolerance or below min_effect_ms threshold |
| `MISSING_IN_BASELINE` | New test case not in baseline |
| `MISSING_IN_LATEST` | Test case removed or not run |

### Thresholds

- **tolerance_pct** (default: 5%): Minimum percentage change to be considered significant
- **min_effect_ms** (default: 5ms): Minimum absolute change to be considered significant

A change is `UNCHANGED` if it's within tolerance% OR below min_effect_ms.

## Baseline Rules

1. **baseline.json is committed to the repository**
2. Update baseline only after deliberately accepting performance changes:
   ```bash
   uv run python bench/compare_results.py --save-baseline
   git add bench/results/baseline.json
   git commit -m "bench: update baseline after optimization"
   ```
3. Never auto-update baseline in CI

## CI Integration

```yaml
- name: Run benchmarks
  run: |
    make release
    uv run python bench/bench.py
```

Exit codes:
- `0`: No regressions
- `1`: Regression detected (CI should fail)
- `2`: Cannot run (missing baseline, bad data, etc.)

## Collecting Profiles

For performance investigation, collect DuckDB query profiles:

```bash
uv run python bench/run_benchmarks.py --profile
```

Profiles are saved to `bench/results/profiles/<case>/query_profile.json`.

## Data Generation

Data is auto-generated on first run. To regenerate manually:

```bash
uv run python bench/generate_data.py
```

Dataset sizes are defined in `bench/config.py`:
- `10k`: 10,000 rows
- `100k`: 100,000 rows
