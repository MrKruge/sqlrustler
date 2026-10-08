# sqlrustler — Build Team Agent Context

You are assisting the **sqlrustler build team** — engineers building an open-source,
high-performance Azure SQL Database export/import tool in Rust.

You have access to 5 specialist agent skills. Read the relevant SKILL.md before
starting work in that area.

## Available skills

- `build-agents/rust-engineer/SKILL.md` — Rust idioms, ownership, async correctness, macro hygiene, API surface
- `build-agents/database-architect/SKILL.md` — SQL type mapping, DDL generation, FK ordering, data round-trip fidelity
- `build-agents/security-auditor/SKILL.md` — credential handling, SQL injection surface, TLS, secret leakage
- `build-agents/performance-engineer/SKILL.md` — throughput, parallelism, memory model, INSERT vs bulk_insert
- `build-agents/qa-engineer/SKILL.md` — test coverage gaps, integration test matrix, release checklist
- `build-agents/devops-engineer/SKILL.md` — CI/CD, cross-compilation, crates.io publish, binary releases

## Project layout

```
sqlrustler/
├── src/
│   ├── main.rs          CLI entry, tracing init
│   ├── lib.rs           pub re-exports
│   ├── cli.rs           clap structs (ConnectArgs, ExportArgs, ImportArgs, BenchArgs)
│   ├── connection.rs    bb8 pool, TiberiusClient type alias
│   ├── schema.rs        DDL extraction, FK topo sort, schema.sql assembly
│   ├── types.rs         SQL Server → Arrow type mapping
│   ├── parquet_writer.rs stream rows → RecordBatch → Parquet
│   ├── archive.rs       ArchiveWriter/ArchiveReader (tar.zst)
│   ├── export.rs        parallel export orchestrator
│   ├── import.rs        parallel import orchestrator
│   ├── bench.rs         benchmark vs sqlpackage
│   └── progress.rs      indicatif helpers
├── tests/integration/
│   └── roundtrip.rs     live DB tests (skipped without SQLRUSTLER_TEST_* env vars)
└── build-agents/        ← you are here
```

## Archive format

```
mydb.rustler  (tar + zstd)
├── manifest.json    version, table list, row counts, identity columns
├── schema.sql       DDL split at "-- SQLRUSTLER_SECTION: POST_DATA"
└── data/
    └── *.parquet    one Parquet file per table, zstd-compressed
```

## Core rules

1. No `unsafe` blocks — flag immediately as P0 if found.
2. No credentials in logs, traces, or test output.
3. `WITH (NOLOCK)` on all export queries — document the dirty-read trade-off.
4. FK constraints are dropped before data load and restored after (POST_DATA section).
5. `SET IDENTITY_INSERT ON/OFF` is session-scoped — each parallel task uses its own connection.
6. All `todo!()` macros must be resolved before v1.0 release.
7. The `.rustler` archive format is append-only in v1 — no random-access modification.
