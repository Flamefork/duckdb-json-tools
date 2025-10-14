#!/usr/bin/env python3
"""Aggregate json_group_merge benchmark results and refresh the summary documentation."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "bench" / "json_group_merge_bench.duckdb"
DEFAULT_SUMMARY_CSV = PROJECT_ROOT / "bench" / "results" / "json_group_merge_summary.csv"
DEFAULT_DOC_PATH = PROJECT_ROOT / "docs" / "benchmarks" / "json_group_merge.md"
MARKER_START = "<!-- BEGIN GENERATED TABLE -->"
MARKER_END = "<!-- END GENERATED TABLE -->"

SUMMARY_QUERY = """
WITH stats AS (
    SELECT
        scenario,
        thread_count,
        implementation,
        AVG(wall_time_ms) AS avg_wall_ms,
        COALESCE(STDDEV_SAMP(wall_time_ms), 0.0) AS std_wall_ms,
        MEDIAN(wall_time_ms) AS median_wall_ms,
        AVG(rows_per_second) AS avg_rps,
        COALESCE(STDDEV_SAMP(rows_per_second), 0.0) AS std_rps,
        MEDIAN(rows_per_second) AS median_rps,
        COUNT(*) AS run_count
    FROM bench_results
    GROUP BY 1, 2, 3
),
paired AS (
    SELECT
        j.scenario,
        j.thread_count,
        j.avg_wall_ms AS json_group_merge_ms,
        j.std_wall_ms AS json_group_merge_std_ms,
        j.median_wall_ms AS json_group_merge_median_ms,
        j.avg_rps AS json_group_merge_rps,
        j.std_rps AS json_group_merge_std_rps,
        j.median_rps AS json_group_merge_median_rps,
        j.run_count AS json_group_merge_runs,
        l.avg_wall_ms AS list_reduce_ms,
        l.std_wall_ms AS list_reduce_std_ms,
        l.median_wall_ms AS list_reduce_median_ms,
        l.avg_rps AS list_reduce_rps,
        l.std_rps AS list_reduce_std_rps,
        l.median_rps AS list_reduce_median_rps,
        l.run_count AS list_reduce_runs
    FROM stats j
    JOIN stats l USING (scenario, thread_count)
    WHERE j.implementation = 'json_group_merge'
      AND l.implementation = 'list_reduce'
)
SELECT
    scenario,
    thread_count,
    json_group_merge_ms,
    list_reduce_ms,
    list_reduce_ms / NULLIF(json_group_merge_ms, 0) AS speedup,
    json_group_merge_std_ms,
    list_reduce_std_ms,
    json_group_merge_median_ms,
    list_reduce_median_ms,
    json_group_merge_rps / 1000.0 AS json_group_merge_krows_s,
    list_reduce_rps / 1000.0 AS list_reduce_krows_s,
    json_group_merge_std_rps / 1000.0 AS json_group_merge_std_krows_s,
    list_reduce_std_rps / 1000.0 AS list_reduce_std_krows_s,
    json_group_merge_median_rps / 1000.0 AS json_group_merge_median_krows_s,
    list_reduce_median_rps / 1000.0 AS list_reduce_median_krows_s,
    json_group_merge_runs,
    list_reduce_runs
FROM paired
ORDER BY scenario, thread_count;
"""


def format_float(value: Optional[float], digits: int = 2) -> str:
    if value is None:
        return ""
    return f"{value:.{digits}f}"


def build_markdown_table(rows: Sequence[Dict[str, object]]) -> str:
    if not rows:
        return "_No benchmark results found. Run the benchmark harness and retry._"
    header = [
        "Scenario",
        "Threads",
        "json_group_merge (ms)",
        "list_reduce (ms)",
        "Speedup",
        "json_group_merge (krows/s)",
        "list_reduce (krows/s)",
    ]
    lines: List[str] = []
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|" + "-|".join(["-" * len(h) for h in header]) + "|")
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["scenario"]),
                    str(row["thread_count"]),
                    format_float(row["json_group_merge_ms"]),
                    format_float(row["list_reduce_ms"]),
                    format_float(row["speedup"]),
                    format_float(row["json_group_merge_krows_s"]),
                    format_float(row["list_reduce_krows_s"]),
                ]
            )
            + " |"
        )
    return "\n" + "\n".join(lines) + "\n"


def write_csv(rows: Sequence[Dict[str, object]], output_path: Path) -> None:
    if not rows:
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def update_document(doc_path: Path, table_markdown: str, timestamp: str) -> None:
    if not doc_path.exists():
        raise FileNotFoundError(f"Documentation file not found: {doc_path}")
    content = doc_path.read_text(encoding="utf-8")
    if MARKER_START not in content or MARKER_END not in content:
        raise ValueError("Document missing generated table markers")
    start_idx = content.index(MARKER_START) + len(MARKER_START)
    end_idx = content.index(MARKER_END)
    updated = content[:start_idx] + table_markdown + content[end_idx:]
    updated = re.sub(
        r"_Last updated:.*_",
        f"_Last updated: {timestamp}_",
        updated,
        count=1,
    )
    doc_path.write_text(updated, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DB_PATH, help="Benchmark DuckDB database path")
    parser.add_argument("--doc", type=Path, default=DEFAULT_DOC_PATH, help="Documentation file to refresh")
    parser.add_argument(
        "--summary-csv",
        type=Path,
        default=DEFAULT_SUMMARY_CSV,
        help="CSV path for aggregated benchmark summary",
    )
    args = parser.parse_args()

    if not args.database.exists():
        raise FileNotFoundError(f"Benchmark database not found: {args.database}")

    connection = duckdb.connect(str(args.database))
    result = connection.execute(SUMMARY_QUERY)
    columns = [desc[0] for desc in result.description]
    rows = [dict(zip(columns, row)) for row in result.fetchall()]
    connection.close()

    write_csv(rows, args.summary_csv)
    table_markdown = build_markdown_table(rows)
    timestamp = dt.datetime.now().isoformat(timespec="seconds")
    update_document(args.doc, table_markdown, timestamp)
    print(f"Wrote {len(rows)} summary rows to {args.summary_csv}")
    print(f"Updated documentation at {args.doc}")


if __name__ == "__main__":
    main()
