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

## Architecture

### Script Relationships

```
bench.py (orchestrator)
├─ ensure_data_exists() → generate_data.py
├─ run_sanity_checks() → sanity_checks.py
├─ run_benchmarks() → run_benchmarks.py
└─ run_comparison() → compare_results.py
```

| Script | Purpose |
|--------|---------|
| `bench.py` | One-command pipeline: generates data, validates, benchmarks, compares |
| `run_benchmarks.py` | Runs benchmarks with filtering/profiling options |
| `compare_results.py` | Compares latest vs baseline, detects regressions |
| `generate_data.py` | Creates deterministic synthetic datasets |
| `sanity_checks.py` | Validates data row counts and schema |
| `config.py` | Centralized configuration (sizes, scenarios, thresholds) |

**When to use which:**
- `bench.py` — Full pipeline, no options. Use for CI and general validation.
- `run_benchmarks.py` — Targeted runs with `--filter` and `--profile`. Use for investigation.

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

## Profiling

DuckDB query profiles are collected via `--profile`. For CPU sampling with Samply
(and `--save-only` to avoid a local server), see `bench/PROFILING.md`.

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

A change is classified as `UNCHANGED` if **either**:
- Absolute change < min_effect_ms, OR
- Percentage change ≤ tolerance_pct

Both conditions protect against noise: small absolute changes in fast queries and small percentage changes in slow queries.

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

## Sanity Checks

Before running benchmarks, `bench.py` validates data integrity:

1. **Row count** — Each file has exactly the expected rows (1k, 10k, 100k)
2. **Schema** — Required columns exist: `json_nested`, `json_flat`, `g1e1`, `g1e3`, `g1e4`

If checks fail, regenerate data:

```bash
uv run python bench/generate_data.py
```

## Data Generation

Data is auto-generated on first run. To regenerate manually:

```bash
uv run python bench/generate_data.py
```

### Dataset Structure

Each parquet file contains:

| Column | Description |
|--------|-------------|
| `json_nested` | Hierarchical JSON with 1-5 levels of nesting |
| `json_flat` | Flattened dot-notation version |
| `g1e1` | Group key with ~10 unique values |
| `g1e3` | Group key with ~1,000 unique values |
| `g1e4` | Group key with ~10,000 unique values |

Data is deterministic (seed=42) and reproducible across runs.

Dataset sizes are defined in `bench/config.py`:
- `1k`: 1,000 rows
- `10k`: 10,000 rows
- `100k`: 100,000 rows

## Adding New Benchmarks

1. **Define scenario in `config.py`:**
   ```python
   SCENARIOS = [
       # ...existing scenarios...
       {"function": "json_new_fn", "scenario": "basic"},
   ]
   ```

2. **Add query builder in `run_benchmarks.py`:**
   ```python
   case "json_new_fn":
       return f"SELECT sum(length(CAST(json_new_fn(json_nested) AS VARCHAR))) FROM {table}"
   ```

3. **Run and save baseline:**
   ```bash
   uv run python bench/run_benchmarks.py --filter json_new_fn
   uv run python bench/compare_results.py --save-baseline
   ```

Cases are auto-discovered from `SIZES × SCENARIOS` (currently 27 cases).
