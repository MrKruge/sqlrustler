# sqlrustler — Technical Overview

This document covers the architecture, design decisions, and internal mechanics of
sqlrustler for contributors and advanced users. For usage instructions see [README.md](README.md).

---

## Problem statement

Microsoft's `sqlpackage` / BACPAC format has three performance ceilings:

1. **Sequential extraction** — tables are exported one at a time. A 100-table database
   runs 100 sequential round-trips regardless of available parallelism.
2. **XML row serialisation** — each row is serialised as XML inside a ZIP file. XML is
   verbose and slow to produce; deflate compression is slow relative to zstd.
3. **ODBC layer** — an extra translation layer between the application and the TDS wire
   protocol adds overhead and blocks some TDS features (e.g. streaming result sets).

sqlrustler removes all three ceilings.

---

## High-level architecture

```
┌──────────────────────────────────────────────────────────┐
│                        sqlrustler                        │
│                                                          │
│  ┌────────┐   ┌──────────────────────────────────────┐  │
│  │ schema │   │         parallel export/import        │  │
│  │extract │   │                                       │  │
│  │        │   │  ┌────────┐  ┌────────┐  ┌────────┐  │  │
│  │sys.    │   │  │table A │  │table B │  │table C │  │  │
│  │catalog │   │  │Parquet │  │Parquet │  │Parquet │  │  │
│  │queries │   │  └────────┘  └────────┘  └────────┘  │  │
│  └────────┘   │       tokio::sync::Semaphore          │  │
│               └──────────────────────────────────────┘  │
│                              │                           │
│                   ┌──────────▼──────────┐                │
│                   │   tar + zstd         │                │
│                   │   .rustler archive   │                │
│                   │  manifest.json       │                │
│                   │  schema.sql          │                │
│                   │  data/*.parquet      │                │
│                   └─────────────────────┘                │
└──────────────────────────────────────────────────────────┘
```

---

## Stack choices

| Concern | Choice | Why |
|---|---|---|
| Runtime | `tokio` (full features) | async I/O for concurrent TDS streams; best ecosystem fit for network-bound work |
| SQL Server protocol | `tiberius` 0.12 | Pure-Rust TDS 7.3 implementation; no ODBC dependency; native async via tokio |
| Connection pooling | `bb8` + `bb8-tiberius` | Async-native pool; each parallel task holds one connection for its full lifetime |
| Columnar format | Apache Arrow + Parquet | Columnar layout = high compression ratio on analytical data; language-agnostic; cloud-readable without sqlrustler |
| Compression | zstd (level 3 default) | ~3× better ratio than deflate at the same speed; single-pass streaming write |
| Archive container | `tar` | Append-only streaming write — no need to seek back to update a central directory (unlike ZIP) |
| CLI | `clap` v4 (derive API) | Env-var fallback, Unicode, subcommand routing, auto-generated `--help` |
| Logging | `tracing` + `tracing-subscriber` | Structured spans; `RUST_LOG` filter at runtime |

---

## Export pipeline

### 1. Schema extraction (`schema.rs`)

A single connection queries the `sys` catalog to build a complete picture of the database:

- `sys.tables` + `sys.schemas` → table list (excludes MS-shipped tables, temporal history shadow tables)
- `sys.columns` + `sys.types` + `sys.identity_columns` → column metadata (excludes computed and hidden columns)
- `sys.foreign_keys` + `sys.foreign_key_columns` → FK graph
- `sys.objects` + `sys.sql_modules` → views, stored procedures, functions

**FK topological sort** (Kahn's algorithm):  
Tables are sorted so parents appear before their children. This guarantees CREATE TABLE
runs in valid dependency order during import. Self-referential FKs are handled — the
`parent_fqn != ref_fqn` guard skips self-edges, and any table not placed by the sort
(circular dependencies) is appended at the end.

**DDL generation** for `schema.sql`:
```
-- DROP FOREIGN KEYS (reverse topo order)        ← runs first on import
-- CREATE TABLES (topo order)
-- VIEWS / STORED PROCS / FUNCTIONS
-- SQLRUSTLER_SECTION: POST_DATA                 ← marker split
-- ADD FOREIGN KEYS (forward topo order)         ← runs after data load
```

The POST_DATA split means FK constraints are never active during the bulk insert phase,
eliminating FK violation errors from parallel out-of-order row insertion.

### 2. Parallel table export (`export.rs`)

```
for each table:
    acquire semaphore permit (max=--parallel)
    spawn tokio task {
        conn = pool.get()
        SELECT [cols] FROM [table] WITH (NOLOCK)
        stream rows → ParquetTableWriter (batch_size rows per RecordBatch)
        write temp .parquet file
        release permit
    }

wait for all tasks
assemble archive: manifest.json + schema.sql + data/*.parquet
```

Key design points:
- **`WITH (NOLOCK)`** on every SELECT — avoids shared lock contention on busy tables.
  Trade-off: may read uncommitted rows during concurrent writes. Documented.
- **Pool size = `parallel + 2`** — the 2 extra connections are reserved for schema
  extraction and archive finalisation.
- **`into_row_stream()`** — tiberius streams rows from the server without buffering the
  full result set. Peak memory per task ≈ `batch_size × row_width × 4`
  (row buffer + ArrowWriter internal buffer).
- **Semaphore permits are `acquire_owned()`** — the permit lives inside the spawned task
  and is dropped when the task completes, releasing the slot for the next table.

### 3. Row → Parquet conversion (`parquet_writer.rs`, `types.rs`)

`sql_to_arrow()` maps every SQL Server type to an Arrow `DataType`. Each column is built
into an Arrow array using the `build_primitive_col!` macro for scalar types or explicit
builders for Decimal128, timestamps, UUIDs, and binary types.

Notable conversions:
- **TINYINT → UInt8** (not Int8) — SQL Server TINYINT is 0–255 unsigned.
- **DATETIMEOFFSET** — converted to UTC before storing as `Timestamp(µs, UTC)`.
- **DECIMAL / NUMERIC** — stored as unscaled `i128` in `Decimal128(precision, scale)`.
  The column scale is preserved exactly; no floating-point involved.
- **UNIQUEIDENTIFIER** — stored as `FixedSizeBinary(16)` raw bytes.
- **XML / GEOGRAPHY / GEOMETRY** — CASTs at the SELECT layer: XML to NVARCHAR(MAX),
  spatial types to WKB bytes. No special tiberius codec required.
- **TIMESTAMP / ROWVERSION** — CAST to VARBINARY(8); exported as raw bytes.

---

## Import pipeline

### 1. Archive read (`archive.rs`)

`ArchiveReader::open()` decompresses the tar.zst and loads all entries into a
`HashMap<String, Vec<u8>>`. The manifest is deserialised from JSON; schema.sql is read
as a UTF-8 string; Parquet bytes are stored keyed by file name.

> **Known limitation (v0.1):** the full archive is in RAM before import starts.
> For a 50 GB database this requires 50 GB of free memory. Streaming read is planned
> for v1.1.

### 2. DDL execution

`schema.sql` is split at `-- SQLRUSTLER_SECTION: POST_DATA`:
- **Pre-data section** is executed first: DROP FK constraints, CREATE TABLE, views, procs.
- **Post-data section** is executed after all rows are loaded: ADD FK constraints.

The `GO` batch separator is handled via a regex split — each batch is sent as an
independent `conn.execute()` call.

### 3. Parallel table import

```
for each table in manifest:
    acquire semaphore permit (max=--parallel)
    spawn tokio task {
        validate_fqn(table_fqn)              ← security: reject malformed FQNs
        conn = pool.get()
        if has_identity: SET IDENTITY_INSERT [table] ON
        open Parquet reader from archive bytes
        for each RecordBatch:
            chunk into 1000-row slices
            build INSERT INTO [table] ([cols]) VALUES (...)
            execute
        SET IDENTITY_INSERT [table] OFF      ← always runs, even on error
        release permit
    }
```

**IDENTITY_INSERT** is session-scoped in SQL Server. Each parallel task has its own
dedicated connection from the pool — this is safe. The OFF is guaranteed to run even if
an INSERT fails, so the connection is clean when returned to the pool.

**Arrow → SQL literal conversion** (`arrow_value_to_sql`):
- NULL → `NULL`
- Strings → `N'...'` with single-quote escaping (`.replace('\'', "''")`)
- Decimals → unscaled i128 split into `int_part.frac_part` with sign preserved for
  values where `abs(raw) < scale_divisor` (e.g. `-0.05` must not become `0.05`)
- Timestamps → `'YYYY-MM-DDTHH:MM:SS.ffffffZ'` (explicit UTC suffix for SQL Server)
- Binary → `0xHEXSTRING`

> **Planned for v1.1:** replace batched INSERT VALUES with tiberius `bulk_insert` (BCP
> protocol) — typically 5–10× faster for large tables because it skips SQL parsing and
> uses minimal logging on the server side.

---

## Security model

### Credential handling

- `ConnectArgs` implements a custom `Debug` that redacts `password` and `aad_token`
  as `<redacted>`. Neither field ever appears in logs or panic output.
- Interactive password prompt uses `rpassword` (no terminal echo).
- Env vars (`SQLRUSTLER_PASSWORD`, `SQLRUSTLER_AAD_TOKEN`) are the recommended delivery
  mechanism — they don't appear in shell history or `ps aux`.

### SQL injection surface

The two places where external values are interpolated into SQL strings:

1. **`table_fqn`** from `manifest.json` — validated against `^\[[\w ]+\]\.\[[\w ]+\]$`
   before any use in SQL. Import aborts immediately if an archive contains a malformed FQN.
2. **`database` name** from `--drop-existing` — validated to word characters only (`^\w+$`).

String values in Arrow arrays use standard SQL single-quote escaping (`''`) — correct for
SQL Server, which does not use backslash escaping.

### TLS

`EncryptionLevel::Required` is set for all connections. `--trust-cert` disables
certificate validation and is documented for development use only.

### Archive integrity

`.rustler` archives have no cryptographic signature in v0.1. The zstd frame provides
integrity against accidental corruption but not against tampering. Only import archives
from trusted sources. Manifest signing is planned for v2.0.

---

## Memory model

| Scenario | Peak RAM |
|---|---|
| Export, 8 parallel, 10k batch, ~200 B/row | ~320 MB |
| Export, 8 parallel, 50k batch, ~200 B/row | ~1.6 GB |
| Import v0.1, 50 GB archive | ~50 GB (full archive in RAM) |
| Import v1.1, streaming archive | ~200 MB |

For export, peak memory is `parallel × batch_size × row_width × 4`. The factor of 4
accounts for the row buffer, the Arrow builder allocations, and the ArrowWriter internal
buffer.

---

## Codebase layout

```
src/
├── main.rs           CLI entry — parse args, init tracing, dispatch subcommand
├── lib.rs            pub re-exports for integration tests
├── cli.rs            clap structs: ConnectArgs (shared), ExportArgs, ImportArgs, BenchArgs
├── connection.rs     bb8 pool builder + auth dispatch (SQL auth / AAD token / interactive)
├── schema.rs         sys catalog queries, FK topo sort, DDL generation, schema.sql assembly
├── types.rs          sql_to_arrow() mapping + select_expr() CAST overrides
├── parquet_writer.rs ParquetTableWriter — pushes tiberius Row → RecordBatch → Parquet
├── archive.rs        ArchiveWriter / ArchiveReader (tar.zst + manifest.json)
├── export.rs         parallel export orchestrator — wires schema + writer + archive
├── import.rs         parallel import orchestrator — reads archive, executes DDL + data
├── bench.rs          benchmark harness — times export, optionally shells out to sqlpackage
└── progress.rs       indicatif helpers (spinner, row-count progress bar)

tests/
└── integration/
    └── roundtrip.rs  export → import → verify row counts (skipped without env vars)
```

---

## Dependency versions (key constraints)

- `tiberius = "0.12"` — required by `bb8-tiberius 0.16`. Do not upgrade tiberius
  independently; check `bb8-tiberius` compatibility first.
- `bigdecimal = "0.3"` — tiberius 0.12 uses bigdecimal 0.3 internally. Version 0.4 is
  a different API and will cause `FromSql` trait resolution failures.
- `arrow = "54"` / `parquet = "54"` — keep these in sync; they are released together.
- `tokio-util` with `compat` feature — required to wrap `tokio::net::TcpStream` in the
  `futures::AsyncRead + AsyncWrite` adaptor that tiberius expects.

---

## Contributing

1. Fork + clone
2. `cargo check` — verify zero errors
3. `cargo clippy -- -D warnings` — clippy must pass clean
4. `cargo test --lib` — unit tests pass without a live DB
5. Set `SQLRUSTLER_TEST_*` env vars and run `cargo test --test integration` against a
   real Azure SQL instance before opening a PR

Core rules:
- No `unsafe` blocks
- No credentials in logs, traces, or test output
- All `todo!()` macros resolved before v1.0
- `WITH (NOLOCK)` on all export SELECT queries — document the trade-off if scope changes

---

## License

MIT OR Apache-2.0 — see [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
