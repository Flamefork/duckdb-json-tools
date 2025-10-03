# DuckDB JSON Tools Extension

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Main Extension Distribution Pipeline](https://github.com/Flamefork/duckdb-json-tools/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/Flamefork/duckdb-json-tools/actions/workflows/MainDistributionPipeline.yml)

A DuckDB extension for high-performance JSON normalization and manipulation.

This extension provides a set of utility functions to work with JSON data, focusing on performance-critical operations that are not covered by DuckDB's core JSON functionality.

**WARNING: This extension is maintained on a best-effort basis by a developer who doesn’t write C++ professionally, so expect rough edges. It hasn’t been hardened for production, and you should validate it in your own environment before relying on it. Feedback and contributions are welcome via GitHub.**  

## Core Features

- **`json_flatten(json)`**: Recursively flattens nested JSON objects and arrays into a single-level object with dot-separated keys.
- **`json_add_prefix(json, text)`**: Adds a string prefix to every top-level key in a JSON object.

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

### `json_flatten(json) -> json`

Rewrites nested JSON structures into a flat, dotted-key object. This is particularly useful for unnesting complex JSON for easier analysis.

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

## Error Handling

- `json_flatten()` returns an error for malformed JSON
- `json_add_prefix()` requires a JSON object (not array or primitive value)
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
