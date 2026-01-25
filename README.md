# DuckDB JSON Tools Extension

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Main Extension Distribution Pipeline](https://github.com/Flamefork/duckdb-json-tools/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/Flamefork/duckdb-json-tools/actions/workflows/MainDistributionPipeline.yml)

A DuckDB extension for high-performance JSON normalization and manipulation.

This extension provides a set of utility functions to work with JSON data, focusing on performance-critical operations that are not covered by DuckDB's core JSON functionality.

**WARNING: This extension is maintained on a best-effort basis by a developer who doesn’t write C++ professionally, so expect rough edges. It hasn’t been hardened for production, and you should validate it in your own environment before relying on it. Feedback and contributions are welcome via GitHub.**  

## Core Features

- **`json_flatten(json[, separator])`**: Recursively flattens nested JSON objects and arrays into a single-level object with path keys (default separator: `.`).
- **`json_add_prefix(json, text)`**: Adds a string prefix to every top-level key in a JSON object.
- **`json_extract_columns(json, columns[, separator])`**: Pulls selected root keys into a struct of `VARCHAR` fields using regex patterns.
- **`json_group_merge(json [ORDER BY ...])`**: Streams JSON patches with RFC 7396 merge semantics without materializing intermediate lists.

## Quick Start

```sql
-- Build from source first (see Installation section)
LOAD './build/release/extension/json_tools/json_tools.duckdb_extension';

-- Flatten nested JSON
SELECT json_flatten('{"user": {"name": "Alice", "age": 30}}');
-- Result: {"user.name":"Alice","user.age":30}

-- Add prefix to keys
SELECT json_add_prefix('{"a": 1, "b": 2}', 'prefix.');
-- Result: {"prefix.a":1,"prefix.b":2}
```

## Installation

> **Note:** This extension is not yet available in the DuckDB community repository. Once published, it will be installable via `INSTALL json_tools FROM community;`

### Current Installation (Development)

This extension must currently be built from source. See the [Development](#development) section below for build instructions.

Once built, load it in DuckDB:

**CLI:**
```shell
duckdb -unsigned
```

```sql
LOAD './build/release/extension/json_tools/json_tools.duckdb_extension';
```

**Python:**
```python
import duckdb
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions': 'true'})
con.execute("LOAD './build/release/extension/json_tools/json_tools.duckdb_extension'")
```

## Usage

### `json_flatten(json[, separator]) -> json`

Rewrites nested JSON structures into a flat object with “path” keys. By default, path segments are joined with `.`.

You can pass an optional `separator` (a 1-character constant `VARCHAR`) to reduce the risk of ambiguous paths when your input keys contain `.`:
```sql
SELECT json_flatten('{"a.b": {"c": 1}}', '/');
```
*Result:*
```json
{"a.b/c":1}
```

No escaping is performed: if your input keys contain the chosen `separator`, the output can be ambiguous and key collisions are possible. Behavior on collisions is not specified.

**Example:**
```sql
SELECT json_flatten('{"outer": {"inner": [1, 2]}}');
```
*Result:*
```json
{"outer.inner.0":1,"outer.inner.1":2}
```

It preserves UTF-8 characters in both keys and values:
```sql
SELECT json_flatten('{"日本語": "こんにちは", "emoji": "🚀"}');
```
*Result:*
```json
{"日本語":"こんにちは","emoji":"🚀"}
```

Empty objects (`{}`) and arrays (`[]`) are omitted from the flattened output as they contain no leaf values:
```sql
SELECT json_flatten('{"data": [], "meta": {}, "count": 5}');
```
*Result:*
```json
{"count":5}
```

### `json_add_prefix(json, prefix) -> json`

Adds a given prefix to all top-level keys in a JSON object. This is useful for avoiding key collisions when merging data from different sources.

**Example:**
```sql
SELECT json_add_prefix('{"a": 1, "b": 2}', 'prefix.');
```
*Result:*
```json
{"prefix.a":1,"prefix.b":2}
```

The prefix is added even to keys whose values are nested objects:
```sql
SELECT json_add_prefix('{"user": {"name": "Alice"}, "count": 5}', 'data_');
```
*Result:*
```json
{"data_user":{"name":"Alice"},"data_count":5}
```

**Note:** This function requires the input to be a JSON object. It will raise an error if given a JSON array or primitive value.

### `json_extract_columns(json, columns[, separator]) -> struct`

Extracts selected root-level fields into a struct of `VARCHAR` columns. The first argument must be a JSON object value (not an array or primitive). `columns` must be a constant JSON object mapping output column names to RE2 regex patterns evaluated against each top-level key (partial matches by default; add anchors to tighten). Patterns are case-sensitive unless you supply inline flags such as `(?i)`. Output columns follow the mapping order.

`separator` defaults to `''` and is inserted between multiple matches for the same column in the order keys appear in the input object. It can be empty but cannot be `NULL` (even when the JSON input is `NULL`). Columns with no matches return `NULL`.

Values are stringified: strings pass through unquoted; arrays, objects, numbers, booleans, and `null` become their JSON text.

**Examples:**
```sql
SELECT (json_extract_columns('{"id": 5, "name": "duck"}',
                             '{"id":"^id$","name":"^name$"}', ',')).id AS id;
-- Result: 5

SELECT (json_extract_columns('{"a":1,"a2":2,"b":3}',
                             '{"a":"^a","b":"^b$"}', '|')).a AS a_values;
-- Result: 1|2

SELECT (json_extract_columns('{"Key": "Value"}',
                             '{"k":"(?i)^key$"}', ',')).k AS case_insensitive;
-- Result: Value

SELECT (json_extract_columns('{"x":"a","xx":"b"}',
                             '{"col":"x"}')).col AS default_separator;
-- Result: ab
```

### `json_group_merge(json_expr [, treat_null_values] [ORDER BY ...]) -> json`

Applies a sequence of JSON patches using [RFC 7396](https://datatracker.ietf.org/doc/html/rfc7396) merge semantics. Inputs can be `JSON` values or `VARCHAR` text that parses as JSON. SQL `NULL` rows are skipped, and the aggregate returns `'{}'::json` when no non-null inputs are provided.

Provide an `ORDER BY` clause to guarantee deterministic results—later rows in the ordered stream overwrite earlier keys, arrays replace wholesale, and `null` removes keys. Pass the optional `treat_null_values` argument to override how object members set to JSON `null` are handled:

- `DELETE NULLS` *(default)* — object members set to `null` delete the key.
- `IGNORE NULLS` — `null` members are dropped before merging, so existing keys stay untouched. Null members in the first patch are also skipped (never seed keys with null values).

The `treat_null_values` argument must be a constant `VARCHAR` literal (case-insensitive, surrounding whitespace ignored).

**Grouped aggregation:**
```sql
SELECT session_id,
       json_group_merge(patch ORDER BY event_ts) AS state
FROM session_events
GROUP BY session_id
ORDER BY session_id;
```

**Window accumulation:**
```sql
SELECT customer_id,
       event_ts,
       json_group_merge(patch ORDER BY event_ts)
           OVER (PARTITION BY customer_id
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS current_state
FROM customer_patch_stream;
```

Use descending order when you want the newest patch first:
```sql
SELECT json_group_merge(patch ORDER BY version DESC) AS latest
FROM config_history
WHERE feature = 'search';
```

Toggle the null handling mode when sparse streams should ignore deletes:
```sql
SELECT json_group_merge(patch, 'IGNORE NULLS' ORDER BY ts)
FROM (VALUES ('{"keep":1}'::json, 1), ('{"keep":null}'::json, 2)) AS t(patch, ts);
```
*Result:*
```json
{"keep":1}
```

## Error Handling

- `json_flatten()` returns an error for malformed JSON
- `json_add_prefix()` requires a JSON object (not array or primitive value)
- `json_extract_columns()` requires a JSON object input and a constant JSON object of string regex patterns; it raises on invalid regexes, NULL separators, non-string object keys, or mismatched input shapes
- `json_group_merge()` surfaces DuckDB JSON parse errors for invalid text and raises on merge buffers that exceed DuckDB limits
- Maximum nesting depth: 1000 levels
- Empty objects (`{}`) and arrays (`[]`) are omitted from flattened output

## Development

### Building from Source

This project uses VCPKG for dependency management.

1.  **Set up VCPKG:**
    ```shell
    git clone https://github.com/Microsoft/vcpkg.git
    ./vcpkg/bootstrap-vcpkg.sh
    export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
    ```
    *(You may want to add the `export` line to your shell's profile, e.g., `~/.bashrc` or `~/.zshrc`)*

2.  **Build the extension:**
    ```shell
    make
    ```

This will create the following binaries in the `./build/release` directory:
- `duckdb`: A shell with the extension pre-loaded.
- `test/unittest`: The test runner.
- `extension/json_tools/json_tools.duckdb_extension`: The distributable extension binary.

### Running Tests

To run the SQL tests:
```shell
make test
```

### Running Benchmarks

Performance benchmarks detect regressions. Run the full suite:

```bash
make release && uv run python bench/bench.py
```

See [bench/README.md](bench/README.md) and [bench/PROFILING.md](bench/PROFILING.md) for filtering, profiling, and baseline management.

## Contributing

Contributions are welcome! Please:
1. Open an issue first to discuss proposed changes
2. Add tests for new functionality in `test/sql/`
3. Run `make test` before submitting a pull request

See [GitHub Issues](https://github.com/Flamefork/duckdb-json-tools/issues) for current tasks and feature requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---
*This extension was originally based on the [DuckDB Extension Template](https://github.com/duckdb/extension-template).*
