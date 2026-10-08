# sqlrustler 🤠

**Open-source, high-performance Azure SQL Database export and import — built in Rust.**

A drop-in replacement for Microsoft's `sqlpackage` / BACPAC workflow that is parallel,
streaming, and typically 5–10× faster. Built by [Danny Kruge](https://github.com/MrKruge).

---

## Why sqlrustler?

`sqlpackage` exports databases as BACPAC files — a ZIP archive of XML-serialised rows
extracted one table at a time over ODBC. For large databases this takes hours.

sqlrustler does it differently:

| | sqlpackage (BACPAC) | sqlrustler |
|---|---|---|
| Protocol | ODBC | Native TDS — no ODBC layer |
| Extraction | Sequential, one table at a time | Parallel — N tables simultaneously |
| Data format | XML row-by-row inside a ZIP | Parquet (columnar, compressed) |
| Container | ZIP + deflate | tar + zstd (~3× better ratio, faster) |
| Memory model | Buffers full tables | Streaming — row buffer capped at ~2 MB/table |
| Open source | No | Yes — MIT / Apache-2.0 |
| Typical speedup | baseline | **5–10× faster** |

---

## Archive format

```
mydb_20261008T120000.rustler      (tar.zst internally)
├── manifest.json                 version, table list, row counts, identity metadata
├── schema.sql                    full DDL — tables, views, procs, FKs
└── data/
    ├── dbo_Customers.parquet
    ├── dbo_Orders.parquet
    └── ...
```

`schema.sql` has a `-- SQLRUSTLER_SECTION: POST_DATA` marker. Everything before it
(CREATE TABLE, views, procs) runs before data load. Everything after (ALTER TABLE ADD
CONSTRAINT) runs after — so FK constraints never block the parallel bulk insert.

The `.rustler` format is a plain tar.zst. You can inspect it with standard tools:

```bash
zstd -d mydb.rustler -o mydb.tar
tar -tf mydb.tar
cat <(tar -xOf mydb.tar manifest.json)
```

---

## Install

### From crates.io

```bash
cargo install sqlrustler
```

### From source (requires Rust 1.80+)

```bash
git clone https://github.com/MrKruge/sqlrustler
cd sqlrustler
cargo build --release
./target/release/sqlrustler --help
```

### Pre-built binaries

Download from [GitHub Releases](https://github.com/MrKruge/sqlrustler/releases):

| Platform | File |
|---|---|
| macOS (Apple Silicon) | `sqlrustler-vX.X.X-aarch64-apple-darwin.tar.gz` |
| macOS (Intel) | `sqlrustler-vX.X.X-x86_64-apple-darwin.tar.gz` |
| Linux x64 | `sqlrustler-vX.X.X-x86_64-unknown-linux-gnu.tar.gz` |
| Windows x64 | `sqlrustler-vX.X.X-x86_64-pc-windows-msvc.zip` |

---

## Quick start

```bash
# 1. Export a database
sqlrustler export \
  -H myserver.database.windows.net \
  -d mydb \
  -u myuser \
  -p mypass \
  -o mydb.rustler

# 2. Import it somewhere else
sqlrustler import \
  -H target.database.windows.net \
  -d mydb_restored \
  -u myuser \
  -p mypass \
  -i mydb.rustler
```

---

## Export

```bash
# SQL Server auth
sqlrustler export \
  -H myserver.database.windows.net \
  -d mydb \
  -u myuser \
  -p mypass \
  -o mydb.rustler

# Azure AD bearer token (recommended for Azure SQL)
export SQLRUSTLER_AAD_TOKEN=$(az account get-access-token \
  --resource https://database.windows.net/ \
  --query accessToken -o tsv)

sqlrustler export \
  -H myserver.database.windows.net \
  -d mydb \
  -o mydb.rustler

# Use env vars instead of flags (keeps credentials out of shell history)
export SQLRUSTLER_HOST=myserver.database.windows.net
export SQLRUSTLER_DATABASE=mydb
export SQLRUSTLER_USER=myuser
export SQLRUSTLER_PASSWORD=mypass

sqlrustler export -o mydb.rustler

# Crank up parallelism for large databases
sqlrustler export -H ... -d ... -j 16 --batch-size 50000 -o mydb.rustler

# Schema DDL only — no row data
sqlrustler export -H ... -d ... --schema-only -o mydb_schema.rustler

# Exclude staging and temp tables
sqlrustler export -H ... -d ... --exclude-tables "tmp_*,staging_*,log_*" -o mydb.rustler

# Maximum zstd compression (slower export, smaller archive)
sqlrustler export -H ... -d ... --compression-level 19 -o mydb.rustler
```

### Export flags

| Flag | Env var | Default | Description |
|---|---|---|---|
| `-H, --host` | `SQLRUSTLER_HOST` | required | SQL Server hostname |
| `-d, --database` | `SQLRUSTLER_DATABASE` | required | Database name |
| `-u, --user` | `SQLRUSTLER_USER` | — | SQL login username |
| `-p, --password` | `SQLRUSTLER_PASSWORD` | — | SQL login password (interactive prompt if omitted) |
| `--aad-token` | `SQLRUSTLER_AAD_TOKEN` | — | Azure AD bearer token (alternative to user/password) |
| `--port` | — | `1433` | TCP port |
| `--trust-cert` | — | `false` | Trust self-signed cert (dev only, never against production) |
| `-o, --output` | — | `<db>_<timestamp>.rustler` | Output archive path |
| `-j, --parallel` | — | CPU count | Tables exported in parallel |
| `--batch-size` | — | `10000` | Rows per Parquet row group |
| `--exclude-tables` | — | — | Comma-separated glob patterns (e.g. `tmp_*,log_*`) |
| `--schema-only` | — | `false` | Export DDL only, no row data |
| `--compression-level` | — | `3` | zstd level: 1 = fastest, 22 = smallest |

---

## Import

```bash
# Standard import (target database must exist)
sqlrustler import \
  -H target.database.windows.net \
  -d mydb_restored \
  -u myuser \
  -p mypass \
  -i mydb.rustler

# Drop and recreate the target database first (clean slate)
sqlrustler import -H ... -d mydb_restored ... --drop-existing -i mydb.rustler

# Data only — schema already exists on the target, just load rows
sqlrustler import -H ... -d mydb_restored ... --data-only -i mydb.rustler

# Tune parallelism
sqlrustler import -H ... -d ... -j 8 --batch-size 2000 -i mydb.rustler
```

### Import flags

| Flag | Env var | Default | Description |
|---|---|---|---|
| `-H, --host` | `SQLRUSTLER_HOST` | required | SQL Server hostname |
| `-d, --database` | `SQLRUSTLER_DATABASE` | required | Database name |
| `-u, --user` | `SQLRUSTLER_USER` | — | SQL login username |
| `-p, --password` | `SQLRUSTLER_PASSWORD` | — | SQL login password |
| `--aad-token` | `SQLRUSTLER_AAD_TOKEN` | — | Azure AD bearer token |
| `--port` | — | `1433` | TCP port |
| `--trust-cert` | — | `false` | Trust self-signed cert |
| `-i, --input` | — | required | Archive to import |
| `-j, --parallel` | — | CPU count | Tables imported in parallel |
| `--batch-size` | — | `1000` | Rows per INSERT batch |
| `--data-only` | — | `false` | Skip DDL, load data only |
| `--drop-existing` | — | `false` | Drop and recreate the target database first |

---

## Benchmark

```bash
# Time sqlrustler export only
sqlrustler bench \
  -H myserver.database.windows.net \
  -d mydb \
  -u myuser \
  -p mypass

# Side-by-side comparison with sqlpackage
# sqlpackage must be on PATH — download: https://aka.ms/sqlpackage-download
sqlrustler bench \
  -H myserver.database.windows.net \
  -d mydb \
  -u myuser \
  -p mypass \
  --compare sqlpackage
```

Example output:

```
=== Benchmark comparison ===
tool             wall(s)    file_size    throughput
---------------------------------------------------
sqlrustler         11.80       320 MB       27 MB/s
sqlpackage         84.50       490 MB        5 MB/s

  speedup: 7.2x (sqlrustler vs sqlpackage)
```

---

## Authentication

sqlrustler supports two auth methods:

**SQL Server auth** — pass `-u` / `-p` flags or set `SQLRUSTLER_USER` / `SQLRUSTLER_PASSWORD`.
If `--password` is omitted and no env var is set, sqlrustler prompts interactively (no echo).

**Azure AD token auth** — obtain a token from the Azure CLI and set `SQLRUSTLER_AAD_TOKEN`:

```bash
export SQLRUSTLER_AAD_TOKEN=$(az account get-access-token \
  --resource https://database.windows.net/ \
  --query accessToken -o tsv)

sqlrustler export -H myserver.database.windows.net -d mydb -o out.rustler
```

> **Security note:** Prefer env vars over `-p` flags — flag values appear in shell history
> and process listings. `SQLRUSTLER_PASSWORD` and `SQLRUSTLER_AAD_TOKEN` are redacted in
> all log output.

---

## SQL Server type mapping

| SQL Server type | Arrow / Parquet type | Notes |
|---|---|---|
| `INT` | `Int32` | |
| `BIGINT` | `Int64` | |
| `SMALLINT` | `Int16` | |
| `TINYINT` | `UInt8` | Unsigned 0–255 |
| `BIT` | `Boolean` | |
| `FLOAT` | `Float64` | |
| `REAL` | `Float32` | |
| `DECIMAL(p,s)` / `NUMERIC(p,s)` | `Decimal128(p,s)` | Full precision preserved |
| `MONEY` | `Decimal128(19,4)` | |
| `SMALLMONEY` | `Decimal128(10,4)` | |
| `VARCHAR(n)` / `CHAR(n)` / `TEXT` | `Utf8` | |
| `NVARCHAR(n)` / `NCHAR(n)` / `NTEXT` | `Utf8` | |
| `VARBINARY(n)` / `BINARY(n)` / `IMAGE` | `Binary` | |
| `DATETIME` / `SMALLDATETIME` / `DATETIME2` | `Timestamp(µs, UTC)` | Stored as UTC |
| `DATETIMEOFFSET` | `Timestamp(µs, UTC)` | Offset applied, stored as UTC |
| `DATE` | `Date32` | Days since 1970-01-01 |
| `TIME` | `Time64(µs)` | |
| `UNIQUEIDENTIFIER` | `FixedSizeBinary(16)` | Raw UUID bytes |
| `XML` | `Utf8` | `CAST(... AS NVARCHAR(MAX))` |
| `GEOGRAPHY` / `GEOMETRY` | `Binary` | WKB via `.STAsBinary()` |
| `TIMESTAMP` / `ROWVERSION` | `Binary` | Raw 8 bytes |

---

## Integration tests

Tests require a live Azure SQL Database and are automatically skipped without env vars:

```bash
export SQLRUSTLER_TEST_HOST=myserver.database.windows.net
export SQLRUSTLER_TEST_DB=testdb
export SQLRUSTLER_TEST_USER=myuser
export SQLRUSTLER_TEST_PASSWORD=mypass

cargo test --test integration
```

Unit tests (no DB required):

```bash
cargo test --lib
```

---

## Building from source

Requirements:
- Rust 1.80+
- A C compiler and `cmake` (required by `aws-lc-rs` via `rustls`)
- On Windows: MSVC build tools

```bash
# Debug build
cargo build

# Optimised release build (~15–30 MB binary)
cargo build --release

# Run clippy
cargo clippy -- -D warnings

# Check for known CVEs
cargo audit
```

---

## Known limitations (v0.1)

- Import loads the full archive into RAM before starting. A 50 GB database needs 50 GB of
  free memory. Streaming import is planned for v1.1.
- Import uses batched `INSERT VALUES` (1000 rows/statement). BCP bulk insert is planned
  for v1.1 and will be 3–5× faster.
- Azure AD device-code interactive login is not yet supported — use `az account
  get-access-token` to obtain a token and pass it via `SQLRUSTLER_AAD_TOKEN`.
- `WITH (NOLOCK)` is used on all export queries for maximum throughput. This may read
  uncommitted rows on a live, write-heavy database. For point-in-time consistent exports,
  quiesce writes or take a database snapshot first.

---

## Roadmap

| Version | Feature |
|---|---|
| v1.1 | BCP bulk insert — 3–5× faster import |
| v1.1 | Streaming archive reader — no full-archive RAM requirement |
| v1.2 | Azure AD device-code flow (MSAL) |
| v1.2 | `list` subcommand — inspect archive contents without extracting |
| v1.2 | Single-table extract — restore one table from an archive |
| v2.0 | Archive checksums and optional manifest signing |
| v2.0 | Resumable export — continue an interrupted run |
| v2.0 | Incremental export — changed rows via CDC / rowversion |
| v2.0 | Direct Azure Blob Storage output |

---

## License

Licensed under either of:

- [MIT License](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.
