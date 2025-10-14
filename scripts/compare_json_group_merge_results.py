#!/usr/bin/env python3
"""Compare two json_group_merge benchmark summary CSVs and report deltas."""

from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class SummaryRow:
    scenario: str
    thread_count: int
    json_group_merge_ms: float
    list_reduce_ms: float
    speedup: float
    json_group_merge_std_ms: float
    list_reduce_std_ms: float
    json_group_merge_median_ms: float
    list_reduce_median_ms: float
    json_group_merge_krows_s: float
    list_reduce_krows_s: float
    json_group_merge_std_krows_s: float
    list_reduce_std_krows_s: float
    json_group_merge_median_krows_s: float
    list_reduce_median_krows_s: float
    json_group_merge_runs: int
    list_reduce_runs: int


REQUIRED_FIELDS = {
    "scenario",
    "thread_count",
    "json_group_merge_ms",
    "list_reduce_ms",
    "speedup",
    "json_group_merge_std_ms",
    "list_reduce_std_ms",
    "json_group_merge_median_ms",
    "list_reduce_median_ms",
    "json_group_merge_krows_s",
    "list_reduce_krows_s",
    "json_group_merge_std_krows_s",
    "list_reduce_std_krows_s",
    "json_group_merge_median_krows_s",
    "list_reduce_median_krows_s",
    "json_group_merge_runs",
    "list_reduce_runs",
}


def parse_float(value: str) -> float:
    return float(value) if value not in ("", None) else 0.0


def read_summary_csv(path: Path) -> Dict[Tuple[str, int], SummaryRow]:
    if not path.exists():
        raise FileNotFoundError(f"Summary CSV not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = {field for field in REQUIRED_FIELDS if field not in reader.fieldnames}  # type: ignore[arg-type]
        if missing:
            raise ValueError(f"CSV is missing required fields {sorted(missing)}: {path}")
        rows: Dict[Tuple[str, int], SummaryRow] = {}
        for raw in reader:
            key = (raw["scenario"], int(raw["thread_count"]))
            rows[key] = SummaryRow(
                scenario=raw["scenario"],
                thread_count=int(raw["thread_count"]),
                json_group_merge_ms=parse_float(raw["json_group_merge_ms"]),
                list_reduce_ms=parse_float(raw["list_reduce_ms"]),
                speedup=parse_float(raw["speedup"]),
                json_group_merge_std_ms=parse_float(raw["json_group_merge_std_ms"]),
                list_reduce_std_ms=parse_float(raw["list_reduce_std_ms"]),
                json_group_merge_median_ms=parse_float(raw["json_group_merge_median_ms"]),
                list_reduce_median_ms=parse_float(raw["list_reduce_median_ms"]),
                json_group_merge_krows_s=parse_float(raw["json_group_merge_krows_s"]),
                list_reduce_krows_s=parse_float(raw["list_reduce_krows_s"]),
                json_group_merge_std_krows_s=parse_float(raw["json_group_merge_std_krows_s"]),
                list_reduce_std_krows_s=parse_float(raw["list_reduce_std_krows_s"]),
                json_group_merge_median_krows_s=parse_float(raw["json_group_merge_median_krows_s"]),
                list_reduce_median_krows_s=parse_float(raw["list_reduce_median_krows_s"]),
                json_group_merge_runs=int(raw["json_group_merge_runs"] or 0),
                list_reduce_runs=int(raw["list_reduce_runs"] or 0),
            )
    return rows


def percent_change(new: float, old: float) -> Optional[float]:
    if old == 0:
        return None
    return (new - old) / old * 100.0


def format_float(value: Optional[float], digits: int = 2, suffix: str = "") -> str:
    if value is None:
        return ""
    if isinstance(value, float) and not math.isfinite(value):
        return "∞" if value > 0 else "-∞"
    return f"{value:.{digits}f}{suffix}"


def render_markdown(rows: Sequence[Sequence[str]], headers: Sequence[str]) -> str:
    lines: List[str] = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "-|".join(["-" * len(h) for h in headers]) + "|")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n" + "\n".join(lines) + "\n"


def format_ratio(value: Optional[float], digits: int = 2) -> str:
    if value is None:
        return ""
    if not math.isfinite(value):
        return "∞" if value > 0 else "-∞"
    return f"{value:.{digits}f}"


def build_comparison_table(
    baseline: Dict[Tuple[str, int], SummaryRow],
    candidate: Dict[Tuple[str, int], SummaryRow],
    tolerance_pct: float,
    sigma_threshold: float,
) -> Tuple[List[List[str]], List[str], List[Tuple[str, str, float]]]:
    headers = [
        "Scenario",
        "Threads",
        "Base ms",
        "New ms",
        "Δ ms",
        "Δ %",
        "Base σ",
        "Δ σ",
        "Base speedup",
        "New speedup",
        "Δ speedup",
        "Base krows/s",
        "New krows/s",
        "Δ krows/s",
    ]
    rows: List[List[str]] = []
    degradations: List[Tuple[str, str, float]] = []
    for key in sorted(set(baseline.keys()) | set(candidate.keys())):
        base_row = baseline.get(key)
        cand_row = candidate.get(key)
        if not base_row or not cand_row:
            status = "missing in baseline" if cand_row and not base_row else "missing in candidate"
            scenario, thread = key
            rows.append([
                scenario,
                str(thread),
                format_float(base_row.json_group_merge_ms if base_row else None),
                format_float(cand_row.json_group_merge_ms if cand_row else None),
                "",
                status,
                format_float(base_row.speedup if base_row else None),
                format_float(cand_row.speedup if cand_row else None),
                "",
                format_float(base_row.json_group_merge_krows_s if base_row else None),
                format_float(cand_row.json_group_merge_krows_s if cand_row else None),
                "",
            ])
            continue
        delta_ms = cand_row.json_group_merge_ms - base_row.json_group_merge_ms
        delta_pct = percent_change(cand_row.json_group_merge_ms, base_row.json_group_merge_ms)
        delta_speedup = cand_row.speedup - base_row.speedup
        delta_krows = cand_row.json_group_merge_krows_s - base_row.json_group_merge_krows_s
        base_sigma = base_row.json_group_merge_std_ms
        sigma_ratio: Optional[float] = None
        if base_sigma > 0:
            sigma_ratio = delta_ms / base_sigma
        row = [
            base_row.scenario,
            str(base_row.thread_count),
            format_float(base_row.json_group_merge_ms),
            format_float(cand_row.json_group_merge_ms),
            format_float(delta_ms),
            format_float(delta_pct, suffix="%"),
            format_float(base_sigma),
            format_ratio(sigma_ratio),
            format_float(base_row.speedup),
            format_float(cand_row.speedup),
            format_float(delta_speedup),
            format_float(base_row.json_group_merge_krows_s),
            format_float(cand_row.json_group_merge_krows_s),
            format_float(delta_krows),
        ]
        rows.append(row)
        if delta_pct is not None and delta_ms > 0:
            beyond_tolerance = delta_pct > tolerance_pct
            if base_sigma > 0:
                sigma_exceeds = abs(delta_ms) >= sigma_threshold * base_sigma
            else:
                sigma_exceeds = abs(delta_ms) > 0
            if beyond_tolerance and sigma_exceeds:
                degradations.append((base_row.scenario, str(base_row.thread_count), delta_pct))
    return rows, headers, degradations


def write_output(table_rows: Sequence[Sequence[str]], headers: Sequence[str], output_path: Optional[Path]) -> None:
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as handle:
            handle.write(render_markdown(table_rows, headers))
    else:
        print(render_markdown(table_rows, headers))


def summarize_regressions(degradations: Sequence[Tuple[str, str, float]]) -> None:
    if degradations:
        print("Detected regressions (latency increases beyond tolerance):")
        for scenario, threads, pct in sorted(degradations, key=lambda item: item[2], reverse=True):
            print(f"  - {scenario} (threads={threads}): +{pct:.2f}% wall time")
    else:
        print("No latency regressions detected beyond configured tolerances.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True, help="Path to baseline summary CSV")
    parser.add_argument("--candidate", type=Path, required=True, help="Path to candidate summary CSV")
    parser.add_argument(
        "--tolerance-pct",
        type=float,
        default=2.0,
        help="Allowed percentage increase in wall time before flagging regression (default: %(default)s)",
    )
    parser.add_argument(
        "--sigma-threshold",
        type=float,
        default=2.0,
        help="Number of baseline standard deviations required to flag regression (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional path to write comparison table as Markdown",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    baseline_rows = read_summary_csv(args.baseline)
    candidate_rows = read_summary_csv(args.candidate)
    table_rows, headers, degradations = build_comparison_table(
        baseline_rows, candidate_rows, args.tolerance_pct, args.sigma_threshold
    )
    write_output(table_rows, headers, args.output)
    summarize_regressions(degradations)


if __name__ == "__main__":
    main()
