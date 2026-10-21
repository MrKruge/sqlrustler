# sqlrustler — Performance Status

**Date:** 2026-10-08  
**Target DB:** `krugesql.database.windows.net / krugedb`  
**Benchmark target:** beat sqlpackage at 14 min 00s

---

## Database profile (krugedb)

| Table | Rows | Type | Chunk strategy |
|---|---|---|---|
| [dbo].[Votes] | 10,143,364 | HEAP | 8 modulo chunks on [Id] |
| [dbo].[AnalysisResults] | 3,946,211 | Indexed | 8 range chunks on [Id] |
| [dbo].[Comments] | 3,875,183 | HEAP | 8 modulo chunks on [Id] |
| [dbo].[Posts] | 3,729,195 | HEAP | 8 modulo chunks on [Id] |
| [dbo].[Badges] | 1,102,019 | HEAP | 8 modulo chunks on [Id] |
| [dbo].[Users] | 299,398 | — | single connection |
| [dbo].[PostLinks] | 161,519 | — | single connection |
| 8 × small DBA/lookup tables | < 100 | — | single connection |

**Total:** ~23M rows across 15 tables.

---

## What we learned (hard way)

### Run 1 — modulo chunks, single global Semaphore(16), 16 parallel
- All 15 tables × 8 chunks = 120 tasks queued on 16 permits
- Large tables starved. Posts/Votes timed out at 120s
- **Result: FAIL (timeout)**

### Run 2 — no modulo (heap tables → single connection), Semaphore(6)
- Heap tables fell back to one sequential scan per table
- Votes at 10M rows / ~14k rows/s = **12 min for Votes alone**
- **Result: FAIL (too slow — 16:42 wall time)**

### Run 3 — no modulo, Semaphore(6), timeout bumped to 300s  
- Same problem — Votes still single connection, still 12 min
- Posts timed out again at 300s (server throttled under combined load)
- **Result: FAIL (timeout)**

### Key insight
Modulo chunking on heap tables **does work** on Azure SQL — but only when the
table gets dedicated connections. Proof: Votes completed in **85s (10.1M rows)**
when 32 connections were dedicated to it alone (observed in pressure tester).

The failure in Run 1 was not modulo itself — it was all tables competing for the
same semaphore simultaneously, starving large tables of connections.

---

## Current approach (Run 4 — in progress)

**Two-semaphore orchestration** (`export.rs`):

```
large_sem = Semaphore(n_large_tables × table_parallelism)
           = 5 × 8 = 40 permits

small_sem = Semaphore(parallel)
           = 8 permits
```

- Large tables (chunked): each gets 8 modulo/range chunk workers, all compete
  only against other large-table chunks via `large_sem`. Zero contention from
  small tables.
- Small tables: share `small_sem(8)` independently.
- Pool size: `(8 × 8) + 8 + 8 = 80` connections headroom.

**Expected wall time:**
- Large tables all fire simultaneously. Wall time ≈ slowest large table.
- Votes at ~119k rows/s (proven) = ~85s
- Posts/Comments/AnalysisResults each ~32–40s (smaller, run concurrently)
- Small tables overlap during large table run
- **Target: < 2 min total**

---

## Run 5 — Two-semaphore, large_sem capped at 16, chunk retry (COMPLETED ✓)

**Wall time: 12 minutes 36 seconds — beats sqlpackage (14:00)**

```
large_sem = Semaphore(min(n_large × tp, 16)) = 16 permits
small_sem = Semaphore(8)
--pressure full: parallel=8, batch_size=100k, table_parallelism=8
```

**Timeline:**
| t | event |
|---|---|
| 0:04 | tbl_DBA_User_level_permissions done (4 rows) |
| 0:07 | Azure Monitor resource ID discovered |
| 5:39 | Wave 1 done: Comments (3.9M), Badges (1.1M), Votes (10.1M), PostTypes, PostLinks, Users all finish simultaneously |
| 12:26 | Posts (3.7M) + AnalysisResults (3.9M) done |
| 12:36 | Archive written, export complete |

**Monitor report highlights:**
- Local CPU avg: **8%** — client not the bottleneck
- Network avg rx: **21.4 MB/s**, peak **31 MB/s** — WAN healthy
- No Azure DTU data (resource group not passed — next run pass `--azure-resource-group DataExposed`)
- Bottleneck verdict: "Azure SQL tier may be the limit" (confirmed by slow Posts/AnalysisResults solo phase)

**Root cause of remaining 7 min:** AnalysisResults is an indexed table running range chunks, but in wave 2 it runs nearly alone after wave 1 finishes. Network drops from ~25 MB/s to ~18 MB/s in the solo phase — Azure SQL is throttling single-table queries differently than parallel multi-table queries.

---

## Pressure tester

New subcommand built today:

```bash
sqlrustler pressure \
  -H krugesql.database.windows.net \
  -d krugedb \
  --azure-auth \
  --max-connections 32 \
  --step-secs 10
```

Three phases:
1. **Local Parquet sink** — finds max local encode/compress throughput (no network)
2. **Network probe** — single-connection stream of largest table, measures WAN MB/s
3. **DB ramp** — ramps 1→2→4→8→16→32 connections, finds the knee (Azure SQL ceiling)

Use this before tuning `--pressure` settings to know which layer is the bottleneck.

---

## Known constraints

- **No schema modifications** — no indexes, no temp tables, no server-side objects
- Source DB is read-only from sqlrustler's perspective
- Running from laptop over WAN (~50–100ms RTT to Azure)
- CPU on client stays < 5% — bottleneck is always network/server, never local

---

## Next steps to push below 8 minutes

The 7-minute AnalysisResults tail is the main target. Options:

1. **Pass `--azure-resource-group DataExposed`** on next run to get DTU/CPU data from
   Azure Monitor — confirms whether server is throttling AnalysisResults specifically.

2. **AnalysisResults is range-chunked (indexed)** — chunks may be uneven if IDs are
   non-contiguous. Add a row-count pre-check per chunk to detect skew:
   ```sql
   SELECT COUNT(*) FROM [dbo].[AnalysisResults] WITH (NOLOCK) WHERE [Id] BETWEEN lo AND hi
   ```

3. **Increase chunk count for AnalysisResults** — currently 8 chunks. If chunks are
   completing fast (per-chunk trace will show this), try 16. The range split is O(1).

4. **Run with RUST_LOG=sqlrustler=debug** to see per-chunk heartbeats and rows/s:
   ```bash
   RUST_LOG=sqlrustler=debug sqlrustler export -H krugesql... --monitor --azure-resource-group DataExposed
   ```
   This will show which specific chunks are slow.

5. **Query Store report** (new — runs automatically with `--monitor`): after next run
   check the Query Store output for avg_duration and avg_logical_io on AnalysisResults
   SELECT queries — high I/O suggests heavy scan, high duration suggests DTU cap.

6. **Forwarded records on heap tables:** If Votes/Comments/Posts have high forwarded
   record counts, modulo scans are slower (extra I/O). Read-only diagnostic:
   ```sql
   SELECT forwarded_record_count
   FROM sys.dm_db_index_physical_stats(DB_ID(), OBJECT_ID('dbo.Votes'), 0, NULL, 'SAMPLED');
   ```
