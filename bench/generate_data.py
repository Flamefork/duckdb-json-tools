#!/usr/bin/env python3
import argparse
import json
import random
import uuid
from datetime import datetime, timezone

import duckdb

from config import (
    ARRAY_SIZES,
    COMPLEXITY_WEIGHTS,
    DATA_DIR,
    FIELDS_PER_LEVEL_MAX,
    FIELDS_PER_LEVEL_MIN,
    NUM_POOL,
    SIZES,
    SPECIAL_KEY_PROB,
    SPECIAL_KEYS,
    STR_POOL,
    UNICODE_STRINGS,
    UNICODE_VALUE_PROB,
)

assert abs(sum(COMPLEXITY_WEIGHTS.values()) - 1.0) < 1e-9, \
    f"COMPLEXITY_WEIGHTS must sum to 1.0, got {sum(COMPLEXITY_WEIGHTS.values())}"


def pick_complexity(row_idx: int, seed: int) -> int:
    """Deterministically pick complexity level 1-5 based on row and seed."""
    rng = random.Random(f"{seed}:{row_idx}:complexity")
    r = rng.random()
    cumulative = 0.0
    for level in sorted(COMPLEXITY_WEIGHTS.keys()):
        cumulative += COMPLEXITY_WEIGHTS[level]
        if r < cumulative:
            return level
    return 5  # fallback


def make_rng(seed: int, row_idx: int, path: str) -> random.Random:
    """Create deterministic RNG for specific field path."""
    return random.Random(f"{seed}:{row_idx}:{path}")


def pick_value(rng: random.Random) -> str | int | bool | None:
    """Pick a random value with type distribution: str 50%, num 30%, bool 15%, null 5%."""
    r = rng.random()
    if r < 0.50:
        if rng.random() < UNICODE_VALUE_PROB:
            return rng.choice(UNICODE_STRINGS)
        return rng.choice(STR_POOL)
    elif r < 0.80:
        return rng.choice(NUM_POOL)
    elif r < 0.95:
        return rng.choice([True, False])
    else:
        return None


def pick_key(rng: random.Random, prefix: str, idx: int) -> str:
    """Pick field key, occasionally with special characters."""
    if rng.random() < SPECIAL_KEY_PROB:
        return rng.choice(SPECIAL_KEYS)
    return f"{prefix}{idx}"


def flatten_document(obj: dict, prefix: str = "") -> dict:
    """Flatten nested document to dot-notation keys."""
    result = {}

    for key, value in obj.items():
        full_key = f"{prefix}.{key}" if prefix else key

        if isinstance(value, dict):
            result.update(flatten_document(value, full_key))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    result.update(flatten_document(item, f"{full_key}[{i}]"))
                else:
                    result[f"{full_key}[{i}]"] = item
        else:
            result[full_key] = value

    return result


def generate_nested_object(
    seed: int,
    row_idx: int,
    path: str,
    current_depth: int,
    max_depth: int,
    complexity: int,
) -> dict:
    """Recursively generate nested object up to max_depth."""
    rng = make_rng(seed, row_idx, path)
    obj = {}

    # Number of fields at this level
    num_fields = rng.randint(FIELDS_PER_LEVEL_MIN, FIELDS_PER_LEVEL_MAX)

    # Counters for field naming
    str_idx = num_idx = bool_idx = null_idx = obj_idx = arr_idx = 1

    for _ in range(num_fields):
        field_rng = make_rng(seed, row_idx, f"{path}:field:{_}")
        value = pick_value(field_rng)

        if value is None:
            key = pick_key(field_rng, "x", null_idx)
            obj[key] = None
            null_idx += 1
        elif isinstance(value, str):
            key = pick_key(field_rng, "s", str_idx)
            obj[key] = value
            str_idx += 1
        elif isinstance(value, int):
            key = pick_key(field_rng, "n", num_idx)
            obj[key] = value
            num_idx += 1
        elif isinstance(value, bool):
            key = pick_key(field_rng, "b", bool_idx)
            obj[key] = value
            bool_idx += 1

    # Add nested object if not at max depth
    if current_depth < max_depth:
        nested_key = f"o{obj_idx}"
        obj[nested_key] = generate_nested_object(
            seed, row_idx, f"{path}.{nested_key}",
            current_depth + 1, max_depth, complexity
        )

    # Add array if complexity >= 3
    if complexity >= 3 and current_depth <= 2:
        arr_min, arr_max = ARRAY_SIZES.get(complexity, (1, 5))
        arr_size = rng.randint(arr_min, arr_max)
        arr_key = f"a{arr_idx}"

        if complexity >= 4 and current_depth == 1:
            # Array of objects
            obj[arr_key] = [
                {
                    "s1": make_rng(seed, row_idx, f"{path}.{arr_key}[{i}].s1").choice(STR_POOL),
                    "n1": make_rng(seed, row_idx, f"{path}.{arr_key}[{i}].n1").choice(NUM_POOL),
                }
                for i in range(arr_size)
            ]
        else:
            # Array of primitives
            obj[arr_key] = [
                make_rng(seed, row_idx, f"{path}.{arr_key}[{i}]").choice(STR_POOL)
                for i in range(arr_size)
            ]

    return obj


def generate_row(row_idx: int, seed: int) -> dict:
    """Generate a single row with nested JSON, flat JSON, and group keys."""
    complexity = pick_complexity(row_idx, seed)
    max_depth = complexity  # complexity 1 = depth 1, etc.

    # Generate unique ID
    doc_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{seed}:{row_idx}:id"))

    # Build nested document
    nested = {"id": doc_id}
    nested.update(
        generate_nested_object(seed, row_idx, "root", 1, max_depth, complexity)
    )

    # Flatten for flat column
    flat = flatten_document(nested)

    return {
        "json_nested": json.dumps(nested),
        "json_flat": json.dumps(flat),
        "g1e1": f"g_{row_idx % 10}",
        "g1e3": f"g_{row_idx % 1000}",
        "g1e4": f"g_{row_idx % 10000}",
    }


def generate_data(size_name: str, num_rows: int, seed: int) -> None:
    print(f"Generating {size_name} ({num_rows:,} rows) with seed={seed}...")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = DATA_DIR / f"events_{size_name}.parquet"

    conn = duckdb.connect()

    batch_size = 50_000
    rows_generated = 0
    temp_table_created = False

    while rows_generated < num_rows:
        batch_rows = min(batch_size, num_rows - rows_generated)
        batch = [generate_row(rows_generated + i, seed) for i in range(batch_rows)]

        conn.execute("""
            CREATE OR REPLACE TEMP TABLE batch (
                json_nested JSON,
                json_flat JSON,
                g1e1 VARCHAR,
                g1e3 VARCHAR,
                g1e4 VARCHAR
            )
        """)
        conn.executemany(
            "INSERT INTO batch VALUES (?, ?, ?, ?, ?)",
            [(r["json_nested"], r["json_flat"], r["g1e1"], r["g1e3"], r["g1e4"]) for r in batch]
        )

        if not temp_table_created:
            conn.execute("CREATE OR REPLACE TABLE data AS SELECT * FROM batch")
            temp_table_created = True
        else:
            conn.execute("INSERT INTO data SELECT * FROM batch")

        rows_generated += batch_rows
        print(f"  {rows_generated:,} / {num_rows:,}")

    generated_at = datetime.now(timezone.utc).isoformat()
    conn.execute(f"""
        COPY data TO '{output_path}' (
            FORMAT PARQUET,
            KV_METADATA {{
                seed: '{seed}',
                size: '{size_name}',
                rows: '{num_rows}',
                generated_at: '{generated_at}'
            }}
        )
    """)
    conn.close()

    print(f"  Saved to {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic benchmark data")
    parser.add_argument(
        "--size",
        choices=list(SIZES.keys()),
        help="Generate only this size (default: all)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for deterministic generation (default: 42)",
    )
    args = parser.parse_args()

    if args.size:
        generate_data(args.size, SIZES[args.size], args.seed)
    else:
        for size_name, num_rows in SIZES.items():
            generate_data(size_name, num_rows, args.seed)


if __name__ == "__main__":
    main()
