# Third-Party Notices

This project is licensed under MIT. The following third-party components are
included in the source tree or in release binaries.

Source distributions should include this file and the referenced or embedded
license notices. Binary distributions should include this file together with
the referenced license files from the build tree.

## Source Tree

### Ada URL parser

- Upstream: https://github.com/ada-url/ada
- Version: 3.4.4
- Files: `src/ada.cpp`, `src/include/ada.h`
- Upstream license: MIT OR Apache-2.0
- License used by this project: MIT

Copyright 2023 Yagiz Nizipli and Daniel Lemire.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

### ankerl::unordered_dense

- Upstream: https://github.com/martinus/unordered_dense
- Version: 4.8.1
- Files: `src/include/ankerl/unordered_dense.h`,
  `src/include/ankerl/stl.h`
- License: MIT

Copyright 2022 Martin Leitner-Ankerl.

The full MIT license text is preserved in the vendored header files.

### DuckDB Extension Template

- Upstream: https://github.com/duckdb/extension-template
- Files: repository build and extension scaffolding
- License: MIT

Copyright 2018-2025 Stichting DuckDB Foundation.

## Release Binaries

DuckDB loadable extension binaries are linked with DuckDB build artifacts and
may include DuckDB third-party components. Notices for DuckDB itself are in the
DuckDB submodule.

### DuckDB

- Upstream: https://github.com/duckdb/duckdb
- Version used by this repository: submodule `duckdb`
- License: MIT
- License file: `duckdb/LICENSE`

Copyright 2018-2025 Stichting DuckDB Foundation.

### RE2

- Upstream: https://github.com/google/re2
- Source in DuckDB: `duckdb/third_party/re2`
- License: BSD-style
- License file: `duckdb/third_party/re2/LICENSE`

Copyright 2009 The RE2 Authors.

### yyjson

- Upstream: https://github.com/ibireme/yyjson
- Source in DuckDB: `duckdb/third_party/yyjson`
- License: MIT
- License file: `duckdb/third_party/yyjson/LICENSE`

Copyright 2020 YaoYuan.

### Mbed TLS

- Upstream: https://github.com/Mbed-TLS/mbedtls
- Source in DuckDB: `duckdb/third_party/mbedtls`
- License: Apache-2.0 OR GPL-2.0-or-later
- License choice for this project: Apache-2.0
- License file: `duckdb/third_party/mbedtls/LICENSE`

Mbed TLS is used through DuckDB's static build artifacts.
