from pathlib import Path

BENCH_DIR = Path(__file__).parent
DATA_DIR = BENCH_DIR / "data"
RESULTS_DIR = BENCH_DIR / "results"
PROFILES_DIR = RESULTS_DIR / "profiles"
PROJECT_ROOT = BENCH_DIR.parent
EXTENSION_PATH = PROJECT_ROOT / "build" / "release" / "extension" / "json_tools" / "json_tools.duckdb_extension"

SIZES = {
    "10k": 10_000,
    "100k": 100_000,
}

DEFAULT_RUNS = 5
DEFAULT_PROFILE_RUNS = 1
DEFAULT_TOLERANCE_PCT = 5
SCHEMA_VERSION = 2
DEFAULT_MIN_EFFECT_MS = 5.0

# Complexity distribution (must sum to 1.0)
COMPLEXITY_WEIGHTS = {
    1: 0.20,  # flat, no nesting
    2: 0.35,  # 2 levels max
    3: 0.25,  # 3 levels, arrays of objects
    4: 0.15,  # 4 levels, nested arrays
    5: 0.05,  # 5 levels, heavy nesting
}

# Value pools
STR_POOL = [f"value_{i}" for i in range(100)]
NUM_POOL = list(range(1000))
UNICODE_STRINGS = ["привет", "你好", "مرحبا", "🎉", "café", "naïve"]
SPECIAL_KEYS = ["key.with.dots", "key with spaces", "key\"quoted\""]

# Field counts per nesting level
FIELDS_PER_LEVEL_MIN = 2
FIELDS_PER_LEVEL_MAX = 5

# Array sizes by complexity
ARRAY_SIZES = {
    3: (1, 5),   # min, max for complexity 3
    4: (3, 10),  # min, max for complexity 4
    5: (5, 15),  # min, max for complexity 5
}

# Probabilities
UNICODE_VALUE_PROB = 0.05
SPECIAL_KEY_PROB = 0.02

SCENARIOS = [
    {"function": "json_flatten", "scenario": "basic"},
    {"function": "json_add_prefix", "scenario": "basic"},
    {"function": "json_extract_columns", "scenario": "few_patterns"},
    {"function": "json_extract_columns", "scenario": "many_patterns"},
    {"function": "json_group_merge", "scenario": "few_groups"},     # g1e1, 10 groups
    {"function": "json_group_merge", "scenario": "medium_groups"},  # g1e3, 1000 groups
    {"function": "json_group_merge", "scenario": "many_groups"},    # g1e4, 10000 groups
    {"function": "json_group_merge", "scenario": "ignore_nulls"},   # g1e3 with IGNORE NULLS
]
