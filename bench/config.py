from pathlib import Path

BENCH_DIR = Path(__file__).parent
DATA_DIR = BENCH_DIR / "data"
RESULTS_DIR = BENCH_DIR / "results"
PROFILES_DIR = RESULTS_DIR / "profiles"
PROJECT_ROOT = BENCH_DIR.parent
EXTENSION_PATH = PROJECT_ROOT / "build" / "release" / "extension" / "json_tools" / "json_tools.duckdb_extension"

SIZES = {
    "1k": 1_000,
    "10k": 10_000,
    "100k": 100_000,
}

DEFAULT_RUNS = 10
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
    {
        # 5 patterns: 2 exact, 2 prefix, 1 suffix
        "function": "json_extract_columns",
        "scenario": "few_patterns",
        "patterns": {
            "s1": "^s1$",           # exact
            "s2": "^s2$",           # exact
            "all_strings": "^s",    # prefix
            "all_numbers": "^n",    # prefix
            "counts": r"_count$",   # suffix (fallback)
        },
    },
    {
        # 14 patterns: 5 exact, 4 prefix, 5 regex/suffix
        "function": "json_extract_columns",
        "scenario": "medium_patterns",
        "patterns": {
            **{f"exact_s{i}": f"^s{i}$" for i in range(1, 6)},   # 5 exact
            **{f"prefix_{c}": f"^{c}" for c in ["n", "o", "g", "a"]},  # 4 prefix
            "nested_s": r"o1\.s\d+",   # regex
            "nested_n": r"o1\.n\d+",   # regex
            "any_nested": r"\.\w+$",   # regex
            "ids": r"_id$",            # suffix
            "counts": r"_count$",      # suffix
        },
    },
    {
        # 100 patterns: 40 exact, 30 prefix, 30 regex
        "function": "json_extract_columns",
        "scenario": "many_patterns",
        "patterns": {
            **{f"exact_{i:02d}": f"^s{i}$" for i in range(1, 41)},      # 40 exact
            **{f"prefix_{i:02d}": f"^p{i:02d}" for i in range(1, 31)},  # 30 prefix
            **{f"regex_{i:02d}": f"r{i:02d}.*" for i in range(1, 31)},  # 30 regex
        },
    },
    {"function": "json_group_merge", "scenario": "few_groups", "group_col": "g1e1", "merge_opts": ""},
    {"function": "json_group_merge", "scenario": "medium_groups", "group_col": "g1e3", "merge_opts": ""},
    {"function": "json_group_merge", "scenario": "many_groups", "group_col": "g1e4", "merge_opts": ""},
    {"function": "json_group_merge", "scenario": "ignore_nulls", "group_col": "g1e3", "merge_opts": ", 'IGNORE NULLS'"},
    {"function": "json_group_merge", "scenario": "delete_nulls", "group_col": "g1e3", "merge_opts": ", 'DELETE NULLS'"},
]
