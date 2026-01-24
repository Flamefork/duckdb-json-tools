#!/usr/bin/env python3
"""One-command benchmark runner.

Usage: uv run python bench/bench.py

Exit codes:
  0 - No regressions
  1 - Regression detected
  2 - Cannot run (missing baseline, sanity check failed, etc.)
"""
import subprocess
import sys
from pathlib import Path

from config import DATA_DIR, SIZES


def ensure_data_exists() -> bool:
    """Check data files exist, generate if missing. Returns True if ready."""
    missing = []
    for size_name in SIZES:
        path = DATA_DIR / f"events_{size_name}.parquet"
        if not path.exists():
            missing.append(size_name)

    if missing:
        print(f"Data files missing: {', '.join(missing)}")
        print("Generating data...")
        result = subprocess.run(
            [sys.executable, Path(__file__).parent / "generate_data.py"],
            cwd=Path(__file__).parent.parent,
        )
        if result.returncode != 0:
            print("Data generation failed", file=sys.stderr)
            return False

    return True


def run_sanity_checks() -> int:
    """Run sanity checks. Returns exit code."""
    from sanity_checks import run_sanity_checks as _run_checks
    return _run_checks()


def run_benchmarks() -> int:
    """Run benchmarks. Returns exit code."""
    from run_benchmarks import main as _run_benchmarks

    # Temporarily replace sys.argv to remove any flags
    old_argv = sys.argv
    sys.argv = [sys.argv[0]]
    try:
        _run_benchmarks()
        return 0
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else 1
    finally:
        sys.argv = old_argv


def run_comparison() -> int:
    """Run comparison. Returns exit code."""
    from compare_results import compare_results as _compare
    from config import RESULTS_DIR, DEFAULT_TOLERANCE_PCT, DEFAULT_MIN_EFFECT_MS

    return _compare(
        baseline_path=RESULTS_DIR / "baseline",
        latest_path=RESULTS_DIR / "latest",
        output_path=RESULTS_DIR / "diff.json",
        tolerance_pct=DEFAULT_TOLERANCE_PCT,
        min_effect_ms=DEFAULT_MIN_EFFECT_MS,
    )


def main() -> int:
    """Main entry point."""
    # Step 1: Ensure data exists
    if not ensure_data_exists():
        return 2

    # Step 2: Sanity checks
    print("Running sanity checks...")
    sanity_result = run_sanity_checks()
    if sanity_result != 0:
        return sanity_result
    print("Sanity checks passed.\n")

    # Step 3: Run benchmarks
    print("Running benchmarks...")
    bench_result = run_benchmarks()
    if bench_result != 0:
        print(f"Benchmark run failed with exit code {bench_result}", file=sys.stderr)
        return 2
    print()

    # Step 4: Compare with baseline
    print("Comparing with baseline...")
    return run_comparison()


if __name__ == "__main__":
    sys.exit(main())
