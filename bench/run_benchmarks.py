#!/usr/bin/env python3
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

import duckdb

from config import (
    DATA_DIR,
    DEFAULT_PROFILE_RUNS,
    DEFAULT_RUNS,
    EXTENSION_PATH,
    PROFILES_DIR,
    RESULTS_DIR,
    SCENARIOS,
    SCHEMA_VERSION,
    SIZES,
)
from environment import collect_environment

DEFAULT_SEED = 42


def create_connection() -> duckdb.DuckDBPyConnection:
    """Create DuckDB connection with json_tools extension loaded."""
    conn = duckdb.connect(config={"allow_unsigned_extensions": True})
    conn.execute(f"LOAD '{EXTENSION_PATH}'")
    return conn


def build_query(function: str, scenario: str, data_path: Path) -> str:
    """Build benchmark query with result consumption wrapper.

    All queries are wrapped with sum(length(CAST(... AS VARCHAR))) to:
    1. Force computation (anti-dead-code-elimination)
    2. Minimize stdout size (single number instead of millions of rows)
    """
    table = f"read_parquet('{data_path}')"

    if function == "json_flatten":
        return f"SELECT sum(length(CAST(json_flatten(json_nested, '.') AS VARCHAR))) FROM {table}"

    if function == "json_add_prefix":
        return f"SELECT sum(length(CAST(json_add_prefix(json_flat, 'pfx_') AS VARCHAR))) FROM {table}"

    if function == "json_extract_columns":
        # Build patterns based on typical flat keys
        if scenario == "few_patterns":
            patterns = {f"col_{i}": f"s{i}" for i in range(1, 6)}
        elif scenario == "many_patterns":
            patterns = {f"s{i}": f"s{i}" for i in range(1, 10)}
            patterns.update({f"n{i}": f"n{i}" for i in range(1, 5)})
            patterns.update({"nested_s": r"o1\.s\d+"})
        else:  # extreme_patterns
            patterns = {f"p_{i:04d}": f"pattern_{i:04d}" for i in range(1000)}

        import json as json_mod
        patterns_json = json_mod.dumps(patterns).replace("'", "''")
        return f"SELECT sum(length(CAST(json_extract_columns(json_flat, '{patterns_json}', '\\n') AS VARCHAR))) FROM {table}"

    if function == "json_group_merge":
        if scenario == "few_groups":
            group_col = "g1e1"
        elif scenario == "medium_groups":
            group_col = "g1e3"
        else:  # ignore_nulls or many_groups
            group_col = "g1e4"

        # Wrap aggregate result in sum(length(...)) via subquery
        if scenario == "ignore_nulls":
            inner = f"SELECT json_group_merge(json_flat, 'IGNORE NULLS') as result FROM {table} GROUP BY {group_col}"
        else:
            inner = f"SELECT json_group_merge(json_flat) as result FROM {table} GROUP BY {group_col}"
        return f"SELECT sum(length(CAST(result AS VARCHAR))) FROM ({inner})"

    raise ValueError(f"Unknown function: {function}")


def collect_duckdb_profile(conn: duckdb.DuckDBPyConnection, query: str, profile_path: Path) -> None:
    """Run query once with DuckDB profiling enabled, save JSON profile to file."""
    profile_path.parent.mkdir(parents=True, exist_ok=True)

    conn.execute("PRAGMA enable_profiling='json'")
    conn.execute("PRAGMA profiling_mode='detailed'")
    conn.execute(f"PRAGMA profiling_output='{profile_path}'")

    conn.execute(query).fetchall()

    conn.execute("PRAGMA disable_profiling")


def run_single_benchmark(conn: duckdb.DuckDBPyConnection, query: str, runs: int) -> dict:
    """Run benchmark query multiple times and collect timing statistics."""
    # Warmup
    conn.execute(query).fetchall()

    times = []
    for _ in range(runs):
        start = time.perf_counter()
        conn.execute(query).fetchall()
        elapsed = (time.perf_counter() - start) * 1000  # ms
        times.append(elapsed)

    return {
        "min_ms": round(min(times), 2),
        "median_ms": round(median(times), 2),
        "max_ms": round(max(times), 2),
    }


def run_benchmarks(
    conn: duckdb.DuckDBPyConnection,
    cases_to_run: list[tuple[str, dict]],
    runs: int,
    profile: bool = False,
) -> list[dict]:
    results = []
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for size, scenario_config in cases_to_run:
        data_path = DATA_DIR / f"events_{size}.parquet"
        if not data_path.exists():
            print(f"Skipping {size}: data file not found")
            continue

        function = scenario_config["function"]
        scenario = scenario_config["scenario"]
        case_id = get_case_id(function, size, scenario)

        print(f"Running {case_id}...", end=" ", flush=True)

        query = build_query(function, scenario, data_path)

        if profile:
            profile_dir = PROFILES_DIR / case_id.replace("/", "_")
            duckdb_profile_path = profile_dir / "query_profile.json"
            print("  Collecting DuckDB profile...", end=" ", flush=True)
            collect_duckdb_profile(conn, query, duckdb_profile_path)
            print(f"saved to {duckdb_profile_path.relative_to(PROFILES_DIR.parent)}")

        try:
            timing = run_single_benchmark(conn, query, runs)
            print(f"{timing['median_ms']:.1f}ms")

            results.append({
                "function": function,
                "scenario": scenario,
                "size": size,
                "min_ms": timing["min_ms"],
                "median_ms": timing["median_ms"],
                "max_ms": timing["max_ms"],
                "runs": runs,
                "timestamp": timestamp,
            })
        except Exception as e:
            print(f"FAILED: {e}")
            raise SystemExit(2)

    return results


def save_results_json(
    results: list[dict],
    path: Path,
    runs: int,
    warmup: bool,
    environment: dict,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    output = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": results[0]["timestamp"] if results else datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "environment": environment,
        "config": {
            "runs": runs,
            "warmup": warmup,
        },
        "results": [
            {
                "id": get_case_id(r["function"], r["size"], r["scenario"]),
                "function": r["function"],
                "scenario": r["scenario"],
                "size": r["size"],
                "min_ms": r["min_ms"],
                "median_ms": r["median_ms"],
                "max_ms": r["max_ms"],
                "runs": r["runs"],
            }
            for r in results
        ],
    }

    with open(path, "w") as f:
        json.dump(output, f, indent=2)


def show_results(results_path: Path) -> None:
    conn = duckdb.connect()
    with open(results_path) as f:
        data = json.load(f)
    conn.sql("CREATE TABLE results AS SELECT * FROM (VALUES " +
             ", ".join(f"('{r['function']}', '{r['scenario']}', '{r['size']}', {r['min_ms']}, {r['median_ms']}, {r['max_ms']})"
                      for r in data["results"]) +
             ") AS t(function, scenario, size, min_ms, median_ms, max_ms)")
    conn.sql("""
        SELECT function, scenario, size,
               round(min_ms, 1) as min_ms,
               round(median_ms, 1) as median_ms,
               round(max_ms, 1) as max_ms
        FROM results
        ORDER BY function, scenario, size
    """).show()


def get_case_id(function: str, size: str, scenario: str) -> str:
    """Build case ID in format: function/size/scenario."""
    return f"{function}/{size}/{scenario}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run benchmarks")
    parser.add_argument("--filter", help="Filter cases by substring (e.g., 10k, json_flatten, few_groups)")
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS, help=f"Number of runs (default: {DEFAULT_RUNS})")
    parser.add_argument("--profile", action="store_true", help="Collect DuckDB query profiles (reduces runs to 1)")
    args = parser.parse_args()

    if not EXTENSION_PATH.exists():
        print(f"Error: Extension not found at {EXTENSION_PATH}")
        print("Run 'make release' first")
        raise SystemExit(2)

    # Build list of (size, scenario) pairs, filtering by prefix if specified
    cases_to_run: list[tuple[str, dict]] = []
    for size in SIZES.keys():
        for scenario_config in SCENARIOS:
            case_id = get_case_id(scenario_config["function"], size, scenario_config["scenario"])
            if args.filter is None or args.filter in case_id:
                cases_to_run.append((size, scenario_config))

    if not cases_to_run:
        print(f"No cases match filter: {args.filter}")
        return

    print(f"Running {len(cases_to_run)} benchmark case(s)")

    runs = args.runs
    if args.profile and args.runs == DEFAULT_RUNS:
        runs = DEFAULT_PROFILE_RUNS
        print(f"Profile mode: reducing runs to {runs}")

    conn = create_connection()
    try:
        results = run_benchmarks(conn, cases_to_run, runs, profile=args.profile)
    finally:
        conn.close()

    if results:
        sizes_used = sorted(set(size for size, _ in cases_to_run))
        environment = collect_environment(DEFAULT_SEED, sizes_used)

        json_path = RESULTS_DIR / "latest.json"
        save_results_json(results, json_path, runs, warmup=True, environment=environment)
        print(f"\nResults saved to {json_path}")

        print()
        show_results(json_path)


if __name__ == "__main__":
    main()
