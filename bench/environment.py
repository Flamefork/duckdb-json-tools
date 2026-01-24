#!/usr/bin/env python3
import os
import platform
import subprocess
from pathlib import Path

import duckdb

from config import COMPLEXITY_WEIGHTS, EXTENSION_PATH


def get_git_commit() -> str:
    """Get current git commit SHA, or 'unknown' if unavailable."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return "unknown"


def get_git_dirty() -> bool:
    """Check for uncommitted changes, or False if git unavailable."""
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent,
        )
        if result.returncode == 0:
            return bool(result.stdout.strip())
    except Exception:
        pass
    return False


def get_cpu_model() -> str:
    """Get CPU model name (best-effort)."""
    system = platform.system()
    try:
        if system == "Darwin":
            result = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        elif system == "Linux":
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("model name"):
                        return line.split(":")[1].strip()
    except Exception:
        pass
    return "unknown"


def get_duckdb_version() -> str:
    """Get DuckDB version from Python API."""
    try:
        return f"v{duckdb.__version__}"
    except Exception:
        return "unknown"


def get_build_type() -> str:
    """Detect build type from EXTENSION_PATH."""
    path_str = str(EXTENSION_PATH)
    if "reldebug" in path_str:
        return "reldebug"
    elif "release" in path_str:
        return "release"
    elif "debug" in path_str:
        return "debug"
    return "unknown"


def collect_environment(seed: int, sizes: list[str]) -> dict:
    """Collect all environment metadata."""
    return {
        "build_type": get_build_type(),
        "os": platform.system(),
        "arch": platform.machine(),
        "cpu_model": get_cpu_model(),
        "cpu_cores": os.cpu_count(),
        "duckdb_version": get_duckdb_version(),
        "git_commit": get_git_commit(),
        "git_dirty": get_git_dirty(),
        "duckdb_python_package_version": duckdb.__version__,
        "dataset": {
            "seed": seed,
            "sizes": sizes,
            "complexity_weights": list(COMPLEXITY_WEIGHTS.values()),
        },
    }
