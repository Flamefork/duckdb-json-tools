#!/usr/bin/env python3
"""Benchmark json_add_prefix extension function against add_prefix_json SQL macro.

The script orchestrates dataset preparation, correctness checks, repeated timing
runs, and profiler capture for the scenarios defined in the benchmark plan.
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "bench" / "json_add_prefix_bench.duckdb"
SETUP_SQL_PATH = PROJECT_ROOT / "bench" / "json_add_prefix_setup.sql"
RESULTS_CSV_PATH = PROJECT_ROOT / "bench" / "results" / "json_add_prefix_vs_macro.csv"
PROFILES_ROOT = PROJECT_ROOT / "build" / "bench" / "profiles"

PROFILER_SETTINGS = json.dumps({
    'OPERATOR_TIMING': True,
    'RESULT_SET_SIZE': True,
    'SYSTEM_PEAK_BUFFER_MEMORY': True
})

JSON_MACRO_SQL = """
CREATE OR REPLACE TEMP MACRO add_prefix_json(v, prefix) AS (
    json_object(
        *
        SELECT prefix || key AS k, value
        FROM json_each(v)
    )
);
"""

# Standard prefix used across all benchmarks
BENCHMARK_PREFIX = 'pr.'


@dataclass(frozen=True)
class Scenario:
    name: str
    table_name: str
    select_sql: str


SCENARIOS: Tuple[Scenario, ...] = (
    Scenario(
        name="events_shallow",
        table_name="bench_events_shallow",
        select_sql="SELECT payload FROM bench_events_shallow",
    ),
    Scenario(
        name="events_shallow_long",
        table_name="bench_events_shallow_long",
        select_sql="SELECT payload FROM bench_events_shallow_long",
    ),
    Scenario(
        name="events_nested",
        table_name="bench_events_nested",
        select_sql="SELECT payload FROM bench_events_nested",
    ),
    Scenario(
        name="events_nested_long",
        table_name="bench_events_nested_long",
        select_sql="SELECT payload FROM bench_events_nested_long",
    ),
    Scenario(
        name="arrays_heavy",
        table_name="bench_arrays_heavy",
        select_sql="SELECT payload FROM bench_arrays_heavy",
    ),
    Scenario(
        name="arrays_heavy_long",
        table_name="bench_arrays_heavy_long",
        select_sql="SELECT payload FROM bench_arrays_heavy_long",
    ),
    Scenario(
        name="worst_case",
        table_name="bench_worst_case",
        select_sql="SELECT payload FROM bench_worst_case",
    ),
)


def load_setup_sql(connection: duckdb.DuckDBPyConnection, sql_path: Path) -> None:
    with sql_path.open("r", encoding="utf-8") as handle:
        sql_script = handle.read()
    for statement in filter(None, (part.strip() for part in sql_script.split(";"))):
        connection.execute(statement)


def ensure_environment(connection: duckdb.DuckDBPyConnection, *, threads: int, memory_limit: Optional[str]) -> None:
    connection.execute("PRAGMA disable_profiling;")
    connection.execute("PRAGMA enable_profiling='json';")
    connection.execute(f"PRAGMA custom_profiling_settings='{PROFILER_SETTINGS}'")
    connection.execute(f"PRAGMA threads={threads};")
    if memory_limit:
        connection.execute(f"PRAGMA memory_limit='{memory_limit}';")
    connection.execute(JSON_MACRO_SQL)

def verify_functional_parity(connection: duckdb.DuckDBPyConnection, scenario: Scenario) -> None:
    query = f"""
        WITH a AS (
            SELECT json_add_prefix(payload, '{BENCHMARK_PREFIX}') AS v FROM {scenario.table_name}
        ),
        b AS (
            SELECT add_prefix_json(payload, '{BENCHMARK_PREFIX}') AS v FROM {scenario.table_name}
        )
        SELECT
            (SELECT COUNT(*) FROM (SELECT * FROM a EXCEPT ALL SELECT * FROM b)) AS diff_ab,
            (SELECT COUNT(*) FROM (SELECT * FROM b EXCEPT ALL SELECT * FROM a)) AS diff_ba;
    """
    diff_ab, diff_ba = connection.execute(query).fetchone()
    if diff_ab or diff_ba:
        raise RuntimeError(
            f"Mismatch detected for scenario '{scenario.name}': diff_ab={diff_ab}, diff_ba={diff_ba}"
        )


def run_select(connection: duckdb.DuckDBPyConnection, select_sql: str, impl: str) -> None:
    sink = "bench_tmp_sink"
    connection.execute(f"DROP TABLE IF EXISTS {sink};")
    target_sql = (
        f"CREATE TEMP TABLE {sink} AS SELECT json_add_prefix(payload, '{BENCHMARK_PREFIX}') AS payload FROM ({select_sql}) t"
        if impl == "json_add_prefix"
        else f"CREATE TEMP TABLE {sink} AS SELECT add_prefix_json(payload, '{BENCHMARK_PREFIX}') AS payload FROM ({select_sql}) t"
    )
    connection.execute(target_sql)
    connection.execute(f"DROP TABLE {sink};")


def collect_profile_metrics(
    connection: duckdb.DuckDBPyConnection, profile_path: Path
) -> Tuple[Optional[float], Optional[float], Optional[int]]:
    if not profile_path.exists():
        return None, None, None
    try:
        payload = json.loads(profile_path.read_text(encoding='utf-8'))
    except json.JSONDecodeError:
        return None, None, None
    latency = payload.get('latency')
    operator_time = payload.get('operator_time')
    system_peak_memory = payload.get('system_peak_buffer_memory')
    return latency, operator_time, system_peak_memory



def benchmark_scenario(
    connection: duckdb.DuckDBPyConnection,
    scenario: Scenario,
    *,
    runs: int,
    warmup: bool,
    thread_count: int,
) -> List[Dict[str, object]]:
    connection.execute("ANALYZE")
    row_count = connection.execute(f"SELECT COUNT(*) FROM {scenario.table_name}").fetchone()[0]
    verify_functional_parity(connection, scenario)

    results: List[Dict[str, object]] = []
    for impl_label in ("json_add_prefix", "add_prefix_json"):
        if warmup:
            connection.execute("PRAGMA disable_profiling;")
            run_select(connection, scenario.select_sql, impl_label)
        for run_id in range(1, runs + 1):
            profile_dir = PROFILES_ROOT / "json_add_prefix" / f"threads_{thread_count}" / scenario.name
            profile_dir.mkdir(parents=True, exist_ok=True)
            profile_path = profile_dir / f"{impl_label}_run{run_id}.json"
            connection.execute("PRAGMA disable_profiling;")
            connection.execute("PRAGMA enable_profiling='json';")
            connection.execute(f"PRAGMA custom_profiling_settings='{PROFILER_SETTINGS}'")
            profile_literal = str(profile_path).replace("'", "''")
            connection.execute(f"PRAGMA profiling_output='{profile_literal}'")

            start = time.perf_counter()
            run_select(connection, scenario.select_sql, impl_label)
            wall_time = time.perf_counter() - start

            latency, operator_time, system_peak_memory = collect_profile_metrics(connection, profile_path)

            results.append(
                {
                    "scenario": scenario.name,
                    "implementation": impl_label,
                    "thread_count": thread_count,
                    "run_id": run_id,
                    "rows": row_count,
                    "wall_time_ms": wall_time * 1000.0,
                    "latency_ms": latency,
                    "operator_time_ms": operator_time,
                    "rows_per_second": row_count / wall_time if wall_time else None,
                    "system_peak_buffer_memory": system_peak_memory,
                    "profile_path": str(profile_path.relative_to(PROJECT_ROOT)),
                }
            )
    return results




def fetch_all_results(connection: duckdb.DuckDBPyConnection) -> List[Dict[str, object]]:
    query = ("SELECT * FROM bench_results ORDER BY thread_count, scenario, implementation, run_id")
    result = connection.execute(query)
    columns = [desc[0] for desc in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]

def write_results_csv(rows: List[Dict[str, object]], output_path: Path) -> None:
    if not rows:
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def persist_results(connection: duckdb.DuckDBPyConnection, rows: List[Dict[str, object]]) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS bench_results (
            scenario VARCHAR,
            implementation VARCHAR,
            thread_count INTEGER,
            run_id INTEGER,
            rows BIGINT,
            wall_time_ms DOUBLE,
            latency_ms DOUBLE,
            operator_time_ms DOUBLE,
            rows_per_second DOUBLE,
            system_peak_buffer_memory BIGINT,
            profile_path VARCHAR
        );
        """
    )
    if rows:
        insert_sql = """
            INSERT INTO bench_results
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        connection.executemany(
            insert_sql,
            [
                (
                    row["scenario"],
                    row["implementation"],
                    row["thread_count"],
                    row["run_id"],
                    row["rows"],
                    row["wall_time_ms"],
                    row["latency_ms"],
                    row["operator_time_ms"],
                    row["rows_per_second"],
                    row["system_peak_buffer_memory"],
                    row["profile_path"],
                )
                for row in rows
            ],
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="DuckDB database file used for benchmarking (default: %(default)s)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=2,
        help="Number of measured runs per scenario and implementation (default: %(default)s)",
    )
    parser.add_argument(
        "--warmup",
        action="store_true",
        help="Execute an unmeasured warm-up run before each benchmark series",
    )
    parser.add_argument(
        "--threads",
        type=int,
        nargs="+",
        default=[1, 8],
        help="One or more DuckDB thread counts to use during benchmarking (default: %(default)s)",
    )
    parser.add_argument(
        "--memory-limit",
        type=str,
        default=None,
        help="Optional memory limit (e.g. '8GB') to enforce during runs",
    )
    parser.add_argument(
        "--skip-setup",
        action="store_true",
        help="Skip running the dataset setup SQL script",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to existing bench_results table instead of truncating",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.database.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(args.database), config={'allow_unsigned_extensions': 'true'})
    local_repo = PROJECT_ROOT / 'build' / 'release' / 'repository'
    connection.execute("INSTALL json")
    connection.execute("LOAD json")
    connection.execute(f"INSTALL json_tools FROM '{local_repo.as_posix()}'")
    connection.execute("LOAD json_tools")
    connection.execute(JSON_MACRO_SQL)

    if not args.skip_setup:
        load_setup_sql(connection, SETUP_SQL_PATH)

    if not args.append:
        connection.execute("DROP TABLE IF EXISTS bench_results;")

    all_rows: List[Dict[str, object]] = []
    thread_counts: List[int] = []
    for thread_value in args.threads:
        if thread_value not in thread_counts:
            thread_counts.append(thread_value)

    for thread_count in thread_counts:
        ensure_environment(connection, threads=thread_count, memory_limit=args.memory_limit)
        for scenario in SCENARIOS:
            scenario_rows = benchmark_scenario(
                connection,
                scenario,
                runs=args.runs,
                warmup=args.warmup,
                thread_count=thread_count,
            )
            all_rows.extend(scenario_rows)

    persist_results(connection, all_rows)
    combined_rows = fetch_all_results(connection)
    write_results_csv(combined_rows, RESULTS_CSV_PATH)
    connection.close()


if __name__ == "__main__":
    main()
