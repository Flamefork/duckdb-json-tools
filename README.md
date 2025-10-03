# json-tools

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension currently focuses on high-performance JSON normalization utilities.


## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/json_tools/json_tools.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `json_tools.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB.

### `json_flatten(json) -> json`

Rewrites nested JSON structures into dotted-key objects:
```
D select json_flatten('{"outer": {"inner": [1, 2]}}') as result;
┌───────────────────────────────────────┐
│                 result                │
│                  json                 │
├───────────────────────────────────────┤
│ {"outer.inner.0":1,"outer.inner.1":2} │
└───────────────────────────────────────┘
```

**Unicode Support**: `json_flatten()` preserves UTF-8 characters in both keys and values:
```sql
SELECT json_flatten('{"日本語": "こんにちは", "emoji": "🚀"}');
-- Result: {"日本語":"こんにちは","emoji":"🚀"}
```

**Empty Container Behavior**: Empty objects and arrays do not have leaf values, so they are omitted from the flattened output:
```sql
SELECT json_flatten('{"data": [], "meta": {}, "count": 5}');
-- Result: {"count":5}
```

### `json_add_prefix(json, prefix) -> json`

Adds a prefix to all top-level keys in a JSON object:
```sql
SELECT json_add_prefix('{"a": 1, "b": 2}', 'prefix.');
-- Result: {"prefix.a":1,"prefix.b":2}

SELECT json_add_prefix('{"user": {"name": "Alice"}, "count": 5}', 'data_');
-- Result: {"data_user":{"name":"Alice"},"data_count":5}
```

Note: This function requires the input to be a JSON object. It will raise an error if given a JSON array or primitive value.

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL json_tools
LOAD json_tools
```
