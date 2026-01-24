#!/usr/bin/env python3
"""Sanity checks for benchmark data files."""
import sys
from pathlib import Path

import duckdb

from config import DATA_DIR, SIZES

REQUIRED_COLUMNS = ["json_nested", "json_flat", "g1e1", "g1e3", "g1e4"]


def check_row_count(conn: duckdb.DuckDBPyConnection, path: Path, expected: int) -> str | None:
    """Check row count matches expected. Returns error message or None."""
    result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()
    actual = result[0]
    if actual != expected:
        return f"File `{path.name}` contains {actual:,} rows, expected {expected:,}"
    return None


def check_schema(conn: duckdb.DuckDBPyConnection, path: Path) -> str | None:
    """Check required columns exist. Returns error message or None."""
    result = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
    columns = {row[0] for row in result}
    missing = set(REQUIRED_COLUMNS) - columns
    if missing:
        return f"File `{path.name}` missing columns: {', '.join(sorted(missing))}"
    return None


def run_sanity_checks() -> int:
    """Run all sanity checks. Returns exit code (0=ok, 2=error)."""
    conn = duckdb.connect()
    errors = []

    for size_name, expected_rows in SIZES.items():
        path = DATA_DIR / f"events_{size_name}.parquet"

        if not path.exists():
            continue  # Missing files handled by bench.py

        # Row count
        if err := check_row_count(conn, path, expected_rows):
            errors.append(err)

        # Schema
        if err := check_schema(conn, path):
            errors.append(err)

    conn.close()

    if errors:
        print("Sanity check failed:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        print("\nRegenerate data: uv run python bench/generate_data.py", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(run_sanity_checks())
