#!/usr/bin/env python3
"""Benchmark json_group_merge aggregate against list_reduce(json_merge_patch) baseline."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_BIN = PROJECT_ROOT / "build" / "release" / "duckdb"
DEFAULT_DB_PATH = PROJECT_ROOT / "bench" / "json_group_merge_bench.duckdb"
SETUP_SQL_PATH = PROJECT_ROOT / "bench" / "json_group_merge_setup.sql"
RESULTS_CSV_PATH = PROJECT_ROOT / "bench" / "results" / "json_group_merge_vs_list_reduce.csv"
PROFILES_ROOT = PROJECT_ROOT / "build" / "bench" / "profiles"

@dataclass(frozen=True)
class Scenario:
    name: str
    table_name: str
    order_clause: Optional[str]
    tags: Tuple[str, ...] = ()


SCENARIOS: Tuple[Scenario, ...] = (
    Scenario("sessions_shallow_asc", "bench_sessions_shallow", "event_ts"),
    Scenario("sessions_shallow_desc", "bench_sessions_shallow", "event_ts DESC"),
    Scenario("sessions_shallow_long", "bench_sessions_shallow_long", "event_ts"),
    Scenario("customers_nested", "bench_customers_nested", "event_ts"),
    Scenario("arrays_replace", "bench_arrays_replace", "event_ts"),
    Scenario("scalars_mix", "bench_scalars_mix", "event_ts"),
    Scenario("large_docs", "bench_large_docs", "event_ts"),
    Scenario("sessions_heavy_docs", "bench_sessions_heavy_docs", "event_ts", ("heavy_docs",)),
)


def require_duckdb_binary() -> None:
    if not DUCKDB_BIN.exists():
        raise FileNotFoundError(
            f"DuckDB binary not found at {DUCKDB_BIN}. "
            "Run `make release` first or adjust the path."
        )


def execute_script(database: Path, script: str) -> None:
    result = subprocess.run(
        [str(DUCKDB_BIN), str(database)],
        input=script,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"DuckDB command failed (exit code {result.returncode}).\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )


def execute_sql(
    database: Path,
    sql: str,
    *,
    cmds: Optional[Sequence[str]] = None,
    json_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    args: List[str] = [str(DUCKDB_BIN)]
    if json_output:
        args.append("-json")
    if cmds:
        for cmd in cmds:
            args.extend(["-cmd", cmd])
    args.append(str(database))
    args.append(sql)
    return subprocess.run(args, capture_output=True, text=True, check=True)


def make_order_clause(scenario: Scenario) -> str:
    return f" ORDER BY {scenario.order_clause}" if scenario.order_clause else ""


def json_group_merge_expr(scenario: Scenario) -> str:
    order_clause = make_order_clause(scenario)
    return f"json_group_merge(patch{order_clause})"


def list_reduce_expr(scenario: Scenario) -> str:
    order_clause = make_order_clause(scenario)
    list_expr = f"list(patch{order_clause}) FILTER (WHERE patch IS NOT NULL)"
    return (
        f"COALESCE(list_reduce({list_expr}, "
        f"lambda acc, p: json_merge_patch(acc, p), '{{}}'::JSON), '{{}}'::JSON)"
    )


def parse_json_output(output: str) -> List[Dict[str, object]]:
    text = output.strip()
    if not text:
        return []
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Failed to parse DuckDB JSON output: {text}") from exc


def verify_functional_parity(database: Path, scenario: Scenario) -> None:
    agg_expr = json_group_merge_expr(scenario)
    baseline_expr = list_reduce_expr(scenario)
    sql = f"""
        WITH input AS (
            SELECT group_id, event_ts, seq, patch
            FROM {scenario.table_name}
        ),
        agg AS (
            SELECT group_id, {agg_expr} AS merged
            FROM input
            GROUP BY group_id
        ),
        baseline AS (
            SELECT group_id, {baseline_expr} AS merged
            FROM input
            GROUP BY group_id
        )
        SELECT
            group_id,
            json(agg.merged) AS agg_json,
            json(baseline.merged) AS baseline_json
        FROM agg
        JOIN baseline USING (group_id)
        ORDER BY group_id;
    """
    result = execute_sql(
        database,
        sql,
        cmds=("LOAD json;", "LOAD json_tools;"),
        json_output=True,
    )
    rows = parse_json_output(result.stdout)
    if not rows:
        raise RuntimeError(f"Unexpected empty result from parity check for {scenario.name}")
    for row in rows:
        agg_json = row.get("agg_json")
        baseline_json = row.get("baseline_json")
        if agg_json is None and baseline_json is None:
            continue
        if (agg_json is None) != (baseline_json is None):
            raise RuntimeError(
                f"Mismatch detected for scenario '{scenario.name}': null disparity for group_id={row.get('group_id')}"
            )
        agg_obj = json.loads(agg_json) if isinstance(agg_json, str) else agg_json
        baseline_obj = json.loads(baseline_json) if isinstance(baseline_json, str) else baseline_json
        if agg_obj != baseline_obj:
            raise RuntimeError(
                f"Mismatch detected for scenario '{scenario.name}', group_id={row.get('group_id')}"
            )


def query_row_count(database: Path, scenario: Scenario) -> int:
    result = execute_sql(
        database,
        f"SELECT COUNT(*) AS row_count FROM {scenario.table_name};",
        json_output=True,
    )
    rows = parse_json_output(result.stdout)
    if not rows:
        raise RuntimeError(f"Unable to fetch row count for {scenario.table_name}")
    return int(rows[0]["row_count"])


def sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return repr(value)


def run_query(
    database: Path,
    scenario: Scenario,
    impl: str,
    *,
    thread_count: int,
    profile_path: Path,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[int]]:
    profile_dir = profile_path.parent
    profile_dir.mkdir(parents=True, exist_ok=True)
    profile_literal = profile_path.as_posix().replace("'", "''")

    cmds = [
        "LOAD json;",
        "LOAD json_tools;",
        f"PRAGMA threads={thread_count};",
        "PRAGMA disable_profiling;",
        f"PRAGMA profiling_output='{profile_literal}';",
        "PRAGMA enable_profiling='json';",
    ]
    target_expr = json_group_merge_expr(scenario) if impl == "json_group_merge" else list_reduce_expr(scenario)
    sql = f"""
        CREATE TEMP TABLE bench_tmp_sink AS
        SELECT group_id, {target_expr} AS merged
        FROM {scenario.table_name}
        GROUP BY group_id;
        DROP TABLE bench_tmp_sink;
        PRAGMA disable_profiling;
    """
    start = time.perf_counter()
    execute_sql(database, sql, cmds=cmds, json_output=False)
    wall_time = time.perf_counter() - start

    if not profile_path.exists():
        return wall_time, None, None, None
    try:
        payload = json.loads(profile_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return wall_time, None, None, None
    latency = payload.get("latency")
    operator_time = payload.get("operator_time")
    system_peak_memory = payload.get("system_peak_buffer_memory")
    return wall_time, latency, operator_time, system_peak_memory


def warmup_query(database: Path, scenario: Scenario, impl: str, *, thread_count: int) -> None:
    cmds = [
        "LOAD json;",
        "LOAD json_tools;",
        f"PRAGMA threads={thread_count};",
        "PRAGMA disable_profiling;",
    ]
    target_expr = json_group_merge_expr(scenario) if impl == "json_group_merge" else list_reduce_expr(scenario)
    sql = f"""
        CREATE TEMP TABLE bench_tmp_warmup AS
        SELECT group_id, {target_expr} AS merged
        FROM {scenario.table_name}
        GROUP BY group_id;
        DROP TABLE bench_tmp_warmup;
        PRAGMA disable_profiling;
    """
    execute_sql(database, sql, cmds=cmds, json_output=False)


def benchmark_scenario(
    database: Path,
    scenario: Scenario,
    *,
    row_count: int,
    runs: int,
    warmup: bool,
    thread_count: int,
) -> List[Dict[str, object]]:
    verify_functional_parity(database, scenario)

    results: List[Dict[str, object]] = []
    for impl_label in ("json_group_merge", "list_reduce"):
        if warmup:
            warmup_query(database, scenario, impl_label, thread_count=thread_count)
        for run_id in range(1, runs + 1):
            profile_path = (
                PROFILES_ROOT
                / "json_group_merge"
                / f"threads_{thread_count}"
                / scenario.name
                / f"{impl_label}_run{run_id}.json"
            )
            wall_time, latency, operator_time, system_peak_memory = run_query(
                database,
                scenario,
                impl_label,
                thread_count=thread_count,
                profile_path=profile_path,
            )
            rows_per_second = (row_count / wall_time) if wall_time else None
            results.append(
                {
                    "scenario": scenario.name,
                    "implementation": impl_label,
                    "thread_count": thread_count,
                    "run_id": run_id,
                    "rows": row_count,
                    "wall_time_ms": wall_time * 1000.0 if wall_time is not None else None,
                    "latency_ms": latency,
                    "operator_time_ms": operator_time,
                    "rows_per_second": rows_per_second,
                    "system_peak_buffer_memory": system_peak_memory,
                    "profile_path": str(profile_path.relative_to(PROJECT_ROOT)),
                }
            )
    return results


def fetch_all_results(database: Path) -> List[Dict[str, object]]:
    result = execute_sql(
        database,
        "SELECT * FROM bench_results ORDER BY thread_count, scenario, implementation, run_id;",
        json_output=True,
    )
    return parse_json_output(result.stdout)


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


def persist_results(database: Path, rows: Iterable[Dict[str, object]], *, append: bool) -> None:
    rows_list = list(rows)
    if not rows_list:
        return
    statements: List[str] = []
    if not append:
        statements.append("DROP TABLE IF EXISTS bench_results;")
    statements.append(
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
        """.strip()
    )
    values = [
        "("
        + ", ".join(
            [
                sql_literal(row["scenario"]),
                sql_literal(row["implementation"]),
                sql_literal(row["thread_count"]),
                sql_literal(row["run_id"]),
                sql_literal(row["rows"]),
                sql_literal(row["wall_time_ms"]),
                sql_literal(row["latency_ms"]),
                sql_literal(row["operator_time_ms"]),
                sql_literal(row["rows_per_second"]),
                sql_literal(row["system_peak_buffer_memory"]),
                sql_literal(row["profile_path"]),
            ]
        )
        + ")"
        for row in rows_list
    ]
    insert_sql = "INSERT INTO bench_results VALUES\n" + ",\n".join(values) + ";"
    statements.append(insert_sql)
    execute_script(database, "\n".join(statements))


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
        help="Optional memory limit (ignored in CLI mode, retained for flag compatibility)",
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
    require_duckdb_binary()

    args.database.parent.mkdir(parents=True, exist_ok=True)
    if not args.skip_setup:
        setup_sql = SETUP_SQL_PATH.read_text(encoding="utf-8")
        execute_script(args.database, setup_sql)

    if not args.append:
        execute_sql(args.database, "DROP TABLE IF EXISTS bench_results;", json_output=False)

    all_rows: List[Dict[str, object]] = []
    unique_threads: List[int] = []
    for thread in args.threads:
        if thread not in unique_threads:
            unique_threads.append(thread)

    row_counts: Dict[str, int] = {}
    for scenario in SCENARIOS:
        row_counts[scenario.name] = query_row_count(args.database, scenario)

    for thread_count in unique_threads:
        for scenario in SCENARIOS:
            scenario_rows = benchmark_scenario(
                args.database,
                scenario,
                row_count=row_counts[scenario.name],
                runs=args.runs,
                warmup=args.warmup,
                thread_count=thread_count,
            )
            all_rows.extend(scenario_rows)

    persist_results(args.database, all_rows, append=args.append)
    combined_rows = fetch_all_results(args.database)
    write_results_csv(combined_rows, RESULTS_CSV_PATH)


if __name__ == "__main__":
    main()
