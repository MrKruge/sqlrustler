use anyhow::{Context, Result};
use futures::StreamExt;
use indicatif::MultiProgress;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::archive::{
    fqn_to_filename, parquet_archive_path, ArchiveWriter, Manifest, ManifestTable,
};
use crate::cli::ExportArgs;
use crate::connection::{build_pool, DbPool};
use crate::monitor;
use crate::parquet_writer::BlockingParquetWriter;
use crate::progress::make_export_bar;
use crate::schema::{self, TableInfo};
use crate::types::select_expr;

/// How to split a large table across parallel connections.
#[derive(Clone, Debug)]
enum ChunkSpec {
    /// WHERE [pk] BETWEEN lo AND hi  — requires an index on pk
    Range { pk: String, lo: i64, hi: i64 },
    /// WHERE [pk] % divisor = remainder  — works on heap tables; relies on SQL Server
    /// scan sharing across concurrent WITH(NOLOCK) allocation-order scans.
    Modulo {
        pk: String,
        divisor: usize,
        remainder: usize,
    },
}

/// Per-export tuning that every table/chunk task needs (kept small so `export_table`
/// stays under clippy's argument limit).
#[derive(Clone, Copy, Debug)]
struct ExportOpts {
    batch_size: usize,
    compression_level: i32,
}

/// Quote a SQL Server identifier with brackets, doubling any embedded `]`.
fn quote_ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// Escape a value for use inside a single-quoted T-SQL string literal.
fn escape_sql_literal(s: &str) -> String {
    s.replace('\'', "''")
}

/// The WHERE predicate selecting exactly the rows of one chunk.
///
/// Modulo chunks use `ABS(pk % d)`: T-SQL `%` keeps the sign of the dividend, so
/// `-7 % 4 = -3` and a plain `pk % d = r` (r in 0..d) would match NO chunk for negative
/// keys (rows silently lost). `ABS(pk % d)` is in `0..d` for every row, so the chunks
/// partition the table. For non-negative keys it selects exactly the same rows as before.
fn chunk_predicate(chunk: &ChunkSpec) -> String {
    match chunk {
        ChunkSpec::Range { pk, lo, hi } => format!("{} BETWEEN {lo} AND {hi}", quote_ident(pk)),
        ChunkSpec::Modulo {
            pk,
            divisor,
            remainder,
        } => {
            format!("ABS({} % {divisor}) = {remainder}", quote_ident(pk))
        }
    }
}

/// Split the inclusive key range `lo..=hi` into at most `n` contiguous, non-overlapping,
/// gap-free inclusive ranges; the last range ends exactly at `hi`. Uses i128 so extreme
/// bigint bounds cannot overflow.
fn split_key_range(lo: i64, hi: i64, n: usize) -> Vec<(i64, i64)> {
    if n == 0 || lo > hi {
        return vec![];
    }
    let n = n as i128;
    let (lo, hi) = (lo as i128, hi as i128);
    let range = hi - lo + 1;
    let chunk_size = (range + n - 1) / n;
    (0..n)
        .map(|i| {
            let a = lo + i * chunk_size;
            let b = (lo + (i + 1) * chunk_size - 1).min(hi);
            (a, b)
        })
        .filter(|(a, b)| a <= b)
        .map(|(a, b)| (a as i64, b as i64))
        .collect()
}

/// Turn NTILE bucket (min, max) pairs (ordered by bucket) into disjoint inclusive ranges.
/// The key may not be unique (heap tables, unindexed fallback `Id` column), in which case
/// a duplicated value can sit on a bucket boundary and appear in two adjacent buckets;
/// without trimming, both chunks would export those rows (duplicates in the archive).
/// A bucket's lower bound is raised to just past the previous bucket's upper bound, and
/// buckets that become empty are dropped (all their rows belong to the previous chunk).
fn disjoint_bucket_ranges(buckets: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut out: Vec<(i64, i64)> = Vec::with_capacity(buckets.len());
    for &(lo, hi) in buckets {
        let lo = match out.last() {
            Some(&(_, prev_hi)) if lo <= prev_hi => match prev_hi.checked_add(1) {
                Some(v) => v,
                None => continue,
            },
            _ => lo,
        };
        if lo <= hi {
            out.push((lo, hi));
        }
    }
    out
}

pub async fn run(mut args: ExportArgs) -> Result<()> {
    let start = std::time::Instant::now();

    let (parallel, batch_size) = args.effective_parallelism();
    tracing::info!(
        "Export pressure: {} parallel tables, {} rows/batch{}",
        parallel,
        batch_size,
        args.pressure
            .as_deref()
            .map(|p| format!(" (--pressure {p})"))
            .unwrap_or_default()
    );

    // 1. Build connection pool.
    // Large tables each get table_parallelism concurrent chunk connections.
    // Small tables share a separate semaphore of size `parallel`.
    // Pool must be large enough for both to run simultaneously without blocking.
    let table_parallelism = args.effective_table_parallelism().max(1);
    let pool_size = (24 + parallel + 8) as u32; // 24 = large_sem cap + small headroom
    let pool = build_pool(&args.conn, pool_size).await?;

    // 2. Extract schema
    tracing::info!(
        "Extracting schema from {}/{}",
        args.conn.host,
        args.conn.database
    );
    let bundle = schema::extract(&pool, &args.exclude_tables).await?;
    tracing::info!("Found {} tables", bundle.tables.len());
    if !args.schema_only {
        let fqns: Vec<String> = bundle.tables.iter().map(|t| t.fqn.clone()).collect();
        if let Some((a, b)) = find_filename_collision(&fqns) {
            anyhow::bail!(
                "Tables {a} and {b} map to the same archive file name; exporting would overwrite one with the other"
            );
        }
    }

    // 3. Determine output path (clone conn.database before args is partially moved)
    let database_name = args.conn.database.clone();
    let output_path = args.output.take().unwrap_or_else(|| {
        let ts = chrono::Utc::now().format("%Y%m%dT%H%M%S");
        format!("{}_{}.sqlrustler", database_name, ts).into()
    });

    // 4. Temp directory for per-table Parquet files
    let tmp_dir = tempfile::tempdir()?;

    // 4b. Start monitor agents if requested
    let monitor_handle = if args.monitor {
        tracing::info!("Starting monitor agents (local, network, Azure SQL)...");
        Some(
            monitor::start(
                args.conn.host.clone(),
                args.conn.database.clone(),
                args.azure_resource_group.clone(),
                5,
            )
            .await,
        )
    } else {
        None
    };

    // 5. Export table data in parallel (skip if schema_only)
    let mut table_stats: Vec<TableExportStat> = Vec::new();

    if !args.schema_only {
        let multi = Arc::new(MultiProgress::new());
        let tp_threshold = args.table_parallelism_threshold;
        let opts = ExportOpts {
            batch_size,
            compression_level: args.compression_level,
        };

        // Pre-plan all chunks.
        let mut sorted_tables = bundle.tables.clone();
        sorted_tables.sort_by_key(|t| {
            if t.chunk_key.is_some() {
                0usize
            } else {
                1usize
            }
        });

        let chunk_plans: Vec<Vec<ChunkSpec>> = {
            let futs: Vec<_> = sorted_tables
                .iter()
                .map(|table| {
                    let pool = pool.clone();
                    let table = table.clone();
                    async move { plan_chunks(&pool, &table, table_parallelism, tp_threshold).await }
                })
                .collect();
            futures::future::try_join_all(futs).await?
        };

        // Per-table semaphores: each large table gets its own Semaphore(table_parallelism)
        // so its chunks compete only among themselves. A global cap caused starvation —
        // whichever table acquired permits first monopolised all slots, leaving other tables
        // at zero. Azure Monitor confirmed huge headroom (az_cpu peak 11.2%, az_io peak 12.2%)
        // so there is no server-side reason to impose a cross-table cap.
        // lob_sem throttles scatter-I/O LOB chunks (modulo-chunked LOB heaps)
        // to avoid thrashing the LOB_DATA buffer pool with too many concurrent random reads.
        // (plan_chunks range-chunks every LOB heap, so in practice no chunk takes lob_sem today.)
        // small_sem bounds the single-connection (unchunked) tables; it is only used by phase 1
        // because phase 2 holds chunked tables only.
        let lob_sem = Arc::new(Semaphore::new(4));
        let small_sem = Arc::new(Semaphore::new(parallel));

        // Two-phase orchestration: fast tables first, then LOB-range tables.
        // Evidence from v10/v11: Posts LOB throughput collapses (195 rows/s) when
        // 40 other queries compete for the same buffer pool. Running Posts after
        // Votes/Comments/AnalysisResults finish (~60s) gives it an uncontested buffer
        // pool and reproduces the v10 4,450 rows/s peak. At that rate Posts' 3.7M rows
        // finish in ~840s alone — but competing with 40 other queries it never warms up.
        // Phase 1: all non-LOB-range chunked tables + smalls (Votes, Comments, AnalysisResults, Badges)
        // Phase 2: LOB-range tables only (Posts)
        let (lob_range_pairs, fast_pairs): (Vec<_>, Vec<_>) = sorted_tables
            .iter()
            .zip(chunk_plans)
            .partition(|(table, chunks)| is_lob_range_table(table, chunks));

        tracing::info!(
            "Orchestration: phase-1 {} fast tables, phase-2 {} LOB-range tables (sequential I/O, run after phase-1)",
            fast_pairs.len(), lob_range_pairs.len()
        );

        // Spawn phase-1 tasks (fast tables — run immediately in parallel).
        let mut handles: Vec<tokio::task::JoinHandle<Result<Vec<TableExportStat>>>> = Vec::new();

        for (table, chunks) in fast_pairs {
            let pool = pool.clone();
            let table = table.clone();
            let tmp_path = tmp_dir.path().to_path_buf();
            let pb = make_export_bar(&multi, &table.fqn);
            // Phase-1 tables are never LOB-range tables (those were partitioned into phase 2),
            // so every chunked table here gets the full table_parallelism.
            let table_sem = Arc::new(Semaphore::new(table_parallelism));
            let lob_sem = lob_sem.clone();
            let small_sem = small_sem.clone();

            let handle = tokio::spawn(async move {
                if chunks.is_empty() {
                    // Small table — single connection, uses small_sem.
                    let mut last_err = None;
                    for attempt in 0..MAX_RETRIES {
                        if attempt > 0 {
                            let delay = retry_delay_secs(10, attempt);
                            tracing::warn!(
                                "{}: failed ({:#}), retry {}/{} in {}s",
                                table.fqn,
                                last_err.as_ref().unwrap(),
                                attempt,
                                MAX_RETRIES - 1,
                                delay
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                            pb.reset();
                        }
                        tracing::debug!("{}: waiting for small_sem permit", table.fqn);
                        let _permit = small_sem.clone().acquire_owned().await?;
                        let chunk_timeout =
                            std::time::Duration::from_secs(SMALL_TABLE_TIMEOUT_SECS);
                        match tokio::time::timeout(
                            chunk_timeout,
                            export_table(
                                pool.clone(),
                                table.clone(),
                                tmp_path.clone(),
                                opts,
                                None,
                                0,
                                pb.clone(),
                            ),
                        )
                        .await
                        {
                            Ok(Ok(stat)) => return Ok(vec![stat]),
                            Ok(Err(e)) => {
                                last_err = Some(e);
                            }
                            Err(_) => {
                                last_err = Some(anyhow::anyhow!(
                                    "{}: wall-clock timeout after {}s — server stalled",
                                    table.fqn,
                                    chunk_timeout.as_secs()
                                ));
                            }
                        }
                    }
                    Err(last_err.expect("MAX_RETRIES >= 1"))
                } else {
                    export_chunked_table(
                        pool,
                        table,
                        tmp_path,
                        opts,
                        chunks,
                        pb,
                        ChunkSems { table_sem, lob_sem },
                    )
                    .await
                }
            });
            handles.push(handle);
        }

        // Await all phase-1 tables before starting phase-2 (LOB-range tables).
        // This gives Posts an uncontested buffer pool — critical for LOB throughput.
        let overlap_lob = std::env::var("SQLRUSTLER_OVERLAP_LOB")
            .map(|v| v == "1")
            .unwrap_or(false);
        if !overlap_lob {
            for handle in handles.drain(..) {
                collect_stats(handle.await??, &mut table_stats, bundle.tables.len());
            }
        }

        // Phase-2: spawn LOB-range tables (Posts) — same chunk logic, now with uncontested buffer pool.
        let mut lob_handles: Vec<tokio::task::JoinHandle<Result<Vec<TableExportStat>>>> =
            Vec::new();
        let lob_range_parallel = std::env::var("SQLRUSTLER_LOB_RANGE_PARALLEL")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(4);
        for (table, chunks) in lob_range_pairs {
            let pool = pool.clone();
            let table = table.clone();
            let tmp_path = tmp_dir.path().to_path_buf();
            let pb = make_export_bar(&multi, &table.fqn);
            let table_sem = Arc::new(Semaphore::new(lob_range_parallel));
            let lob_sem = lob_sem.clone();
            lob_handles.push(tokio::spawn(async move {
                export_chunked_table(
                    pool,
                    table,
                    tmp_path,
                    opts,
                    chunks,
                    pb,
                    ChunkSems { table_sem, lob_sem },
                )
                .await
            }));
        }
        for handle in handles {
            collect_stats(handle.await??, &mut table_stats, bundle.tables.len());
        }
        for handle in lob_handles {
            collect_stats(handle.await??, &mut table_stats, bundle.tables.len());
        }
    }

    // Never write an archive that silently omits a table.
    if !args.schema_only {
        check_all_tables_exported(bundle.tables.len(), table_stats.len())?;
    }

    // 6. Assemble archive
    tracing::info!("Writing archive to {}", output_path.display());
    let mut archive = ArchiveWriter::create(&output_path, args.compression_level)?;
    archive.write_schema_sql(&bundle.schema_sql)?;

    let total_rows: u64 = table_stats.iter().map(|s| s.rows).sum();
    let manifest = Manifest {
        sqlrustler_version: env!("CARGO_PKG_VERSION").to_string(),
        format_version: 2,
        created_at: chrono::Utc::now().to_rfc3339(),
        source_host: args.conn.host.clone(),
        source_database: args.conn.database.clone(),
        total_rows,
        schema_only: args.schema_only,
        tables: table_stats
            .iter()
            .map(|s| ManifestTable {
                table_fqn: s.table_fqn.clone(),
                file_names: s.archive_paths.clone(),
                file_name: String::new(),
                row_count: s.rows,
                has_identity: s.has_identity,
                identity_columns: s.identity_columns.clone(),
            })
            .chain(if args.schema_only {
                bundle
                    .tables
                    .iter()
                    .map(|t| ManifestTable {
                        table_fqn: t.fqn.clone(),
                        file_names: vec![parquet_archive_path(&t.fqn)],
                        file_name: String::new(),
                        row_count: 0,
                        has_identity: !t.identity_columns.is_empty(),
                        identity_columns: t.identity_columns.clone(),
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
            } else {
                vec![].into_iter()
            })
            .collect(),
    };
    archive.write_manifest(&manifest)?;

    for stat in &table_stats {
        for (local, archive_path) in stat.local_filenames.iter().zip(stat.archive_paths.iter()) {
            let parquet_path = tmp_dir.path().join(local);
            archive.add_parquet_file(archive_path, &parquet_path)?;
        }
    }
    archive.finish()?;

    // 7. Report stats
    let elapsed = start.elapsed();
    let archive_size = std::fs::metadata(&output_path)?.len();
    let throughput = if elapsed.as_secs_f64() > 0.0 {
        (archive_size as f64 / elapsed.as_secs_f64()) as u64
    } else {
        0
    };

    tracing::info!(
        "Export complete: {} rows, {} tables, {:.1}s elapsed, {} archive ({}/s)",
        total_rows,
        table_stats.len(),
        elapsed.as_secs_f64(),
        bytesize::ByteSize(archive_size),
        bytesize::ByteSize(throughput),
    );

    // 8. Optional validation
    if args.validate && !args.schema_only {
        validate_row_counts(&pool, &table_stats).await?;
    }

    // 9. Stop monitor agents and print report
    if let Some(handle) = monitor_handle {
        let samples = handle.finish().await;
        monitor::print_report(&samples);
    }

    // 10. Query Store post-export report (always runs when monitor is on, read-only)
    if args.monitor {
        monitor::query_store_report(&pool).await;
    }

    Ok(())
}

async fn validate_row_counts(pool: &DbPool, stats: &[TableExportStat]) -> Result<()> {
    tracing::info!("Validating row counts against live database...");
    let mut conn = pool
        .get()
        .await
        .context("Failed to get connection for validation")?;
    let mut mismatches = 0usize;

    for stat in stats {
        let sql = format!("SELECT COUNT_BIG(*) FROM {} WITH (NOLOCK)", stat.table_fqn);
        let rows = conn.query(&sql, &[]).await?.into_results().await?;
        let live_count: i64 = rows
            .into_iter()
            .flatten()
            .next()
            .and_then(|r| r.get::<i64, _>(0))
            .unwrap_or(0);

        if live_count as u64 != stat.rows {
            tracing::warn!(
                "MISMATCH {}: archived {} rows, live DB has {} rows",
                stat.table_fqn,
                stat.rows,
                live_count
            );
            mismatches += 1;
        } else {
            tracing::info!("  ✓ {} — {} rows", stat.table_fqn, stat.rows);
        }
    }

    if mismatches > 0 {
        anyhow::bail!("Validation failed: {} table(s) have row count mismatches (possible dirty reads during export)", mismatches);
    }

    tracing::info!("Validation passed — all {} tables match", stats.len());
    Ok(())
}

const MAX_RETRIES: usize = 5;
/// Wall-clock backstop for a single-connection (unchunked) table attempt.
const SMALL_TABLE_TIMEOUT_SECS: u64 = 300;
/// Wall-clock backstop for one chunk attempt (real stalls are caught by the idle detectors).
const CHUNK_TIMEOUT_SECS: u64 = 1200;

/// Backoff before retry number `attempt` (1-based): `base_secs * attempt`.
fn retry_delay_secs(base_secs: u64, attempt: usize) -> u64 {
    base_secs * attempt as u64
}

fn has_lob_column(table: &TableInfo) -> bool {
    table.columns.iter().any(|c| {
        // max_length == -1 means nvarchar(max)/varchar(max)/varbinary(max)
        c.max_length == -1
            || matches!(
                c.sql_type.to_lowercase().as_str(),
                "text" | "ntext" | "image"
            )
    })
}

/// A chunked table with a LOB column that is split by key ranges (e.g. Posts): run in phase 2.
fn is_lob_range_table(table: &TableInfo, chunks: &[ChunkSpec]) -> bool {
    !chunks.is_empty()
        && has_lob_column(table)
        && chunks.iter().any(|c| matches!(c, ChunkSpec::Range { .. }))
}

/// Two different tables whose archive file stems collide would overwrite each other
/// (e.g. `[a_b].[c]` and `[a].[b_c]` both map to `a_b_c`). Returns the first colliding pair.
fn find_filename_collision(fqns: &[String]) -> Option<(String, String)> {
    let mut seen: std::collections::HashMap<String, &String> = std::collections::HashMap::new();
    for fqn in fqns {
        if let Some(prev) = seen.insert(fqn_to_filename(fqn), fqn) {
            return Some((prev.clone(), fqn.clone()));
        }
    }
    None
}

/// Combine per-chunk results (already in chunk order) into one table entry.
fn merge_parts(table: &TableInfo, parts: Vec<TableExportStat>) -> TableExportStat {
    let mut merged = TableExportStat {
        table_fqn: table.fqn.clone(),
        local_filenames: Vec::new(),
        archive_paths: Vec::new(),
        rows: 0,
        has_identity: !table.identity_columns.is_empty(),
        identity_columns: table.identity_columns.clone(),
    };
    for p in parts {
        merged.rows += p.rows;
        merged.local_filenames.extend(p.local_filenames);
        merged.archive_paths.extend(p.archive_paths);
    }
    merged
}

fn collect_stats(
    stats: Vec<TableExportStat>,
    table_stats: &mut Vec<TableExportStat>,
    total_tables: usize,
) {
    for stat in stats {
        tracing::info!(
            "  ✓ {} — {} rows ({}/{} tables done)",
            stat.table_fqn,
            stat.rows,
            table_stats.len() + 1,
            total_tables
        );
        table_stats.push(stat);
    }
}

/// Every table must have produced a stat; otherwise the manifest would silently omit data.
fn check_all_tables_exported(expected: usize, actual: usize) -> Result<()> {
    if expected != actual {
        anyhow::bail!(
            "Export incomplete: {actual} of {expected} tables were exported — refusing to write a partial archive"
        );
    }
    Ok(())
}

/// Semaphores bounding concurrent chunk streams of one table.
struct ChunkSems {
    /// Per-table cap on concurrent chunk connections.
    table_sem: Arc<Semaphore>,
    /// Shared cap on scatter-I/O (modulo-chunked LOB) streams across tables.
    lob_sem: Arc<Semaphore>,
}

/// Fan a chunked table out over parallel connections, retrying each chunk independently.
/// Returns an error if ANY chunk exhausts its retries (so the export can never succeed with a
/// missing chunk). Parts are returned in chunk order.
async fn export_chunked_table(
    pool: DbPool,
    table: TableInfo,
    tmp_path: std::path::PathBuf,
    opts: ExportOpts,
    chunks: Vec<ChunkSpec>,
    pb: indicatif::ProgressBar,
    sems: ChunkSems,
) -> Result<Vec<TableExportStat>> {
    // Each chunk acquires a slot from this table's own semaphore so no table can starve another.
    // Modulo-chunked LOB heaps (scattered random I/O against LOB_DATA) additionally acquire a
    // slot from the shared lob_sem to bound total concurrent LOB scatter reads.
    let has_lob = has_lob_column(&table);
    let mut part_handles = Vec::with_capacity(chunks.len());
    for (i, chunk) in chunks.into_iter().enumerate() {
        let use_lob_sem = has_lob && !matches!(chunk, ChunkSpec::Range { .. });
        let sem = sems.table_sem.clone();
        let lob_sem = sems.lob_sem.clone();
        let pool = pool.clone();
        let table = table.clone();
        let tmp_path = tmp_path.clone();
        let pb = pb.clone();
        part_handles.push(tokio::spawn(async move {
            let mut last_err = None;
            for attempt in 0..MAX_RETRIES {
                if attempt > 0 {
                    // Streams use dedicated connections, so there is nothing to evict;
                    // a short backoff is enough.
                    let delay = retry_delay_secs(5, attempt);
                    tracing::warn!(
                        "{} chunk {}: failed ({:#}), retry {}/{} in {}s",
                        table.fqn,
                        i,
                        last_err.as_ref().unwrap(),
                        attempt,
                        MAX_RETRIES - 1,
                        delay
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                    pb.reset();
                }
                tracing::debug!("{} chunk {}: waiting for table_sem permit", table.fqn, i);
                let _permit = sem.clone().acquire_owned().await?;
                let _lob_permit = if use_lob_sem {
                    Some(lob_sem.clone().acquire_owned().await?)
                } else {
                    None
                };
                tracing::debug!(
                    "{} chunk {}: permit(s) acquired, starting export",
                    table.fqn,
                    i
                );
                // Wall-clock cap is a backstop only; real stalls are caught by the
                // 300s idle row-stream timeout and the 120s command timeout. A healthy
                // chunk under 8-way contention (e.g. Comments, ~250s) can exceed 300s.
                let chunk_timeout = std::time::Duration::from_secs(CHUNK_TIMEOUT_SECS);
                match tokio::time::timeout(
                    chunk_timeout,
                    export_table(
                        pool.clone(),
                        table.clone(),
                        tmp_path.clone(),
                        opts,
                        Some(chunk.clone()),
                        i,
                        pb.clone(),
                    ),
                )
                .await
                {
                    Ok(Ok(stat)) => return Ok(stat),
                    Ok(Err(e)) => last_err = Some(e),
                    Err(_) => {
                        last_err = Some(anyhow::anyhow!(
                            "{} chunk {}: wall-clock timeout after {}s — server stalled",
                            table.fqn,
                            i,
                            chunk_timeout.as_secs()
                        ))
                    }
                }
            }
            let err = last_err.expect("MAX_RETRIES >= 1");
            tracing::error!(
                "{} chunk {}: all {} retries exhausted — giving up. Last error: {:#}",
                table.fqn,
                i,
                MAX_RETRIES,
                err
            );
            Err(err)
        }));
    }
    let mut parts = Vec::with_capacity(part_handles.len());
    for h in part_handles {
        // `??`: a panicked chunk task (JoinError) or an exhausted chunk both fail the table.
        parts.push(h.await??);
    }
    Ok(vec![merge_parts(&table, parts)])
}

/// Unique suffix for temp parquet files so a retry never shares a file with a previous
/// attempt whose writer thread may still be draining.
static TMP_FILE_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Debug)]
struct TableExportStat {
    table_fqn: String,
    /// One entry per part file (single-connection = 1 entry, chunked = N entries).
    local_filenames: Vec<String>,
    archive_paths: Vec<String>,
    rows: u64,
    has_identity: bool,
    identity_columns: Vec<String>,
}

async fn export_table(
    pool: crate::connection::DbPool,
    table: TableInfo,
    tmp_dir: std::path::PathBuf,
    opts: ExportOpts,
    chunk: Option<ChunkSpec>,
    part_index: usize,
    pb: indicatif::ProgressBar,
) -> Result<TableExportStat> {
    let ExportOpts {
        batch_size,
        compression_level,
    } = opts;
    let chunk_label = match &chunk {
        Some(ChunkSpec::Modulo {
            remainder, divisor, ..
        }) => format!("chunk {remainder}/{divisor}"),
        Some(ChunkSpec::Range { lo, hi, .. }) => format!("range {lo}..{hi}"),
        None => "single".to_string(),
    };
    tracing::debug!("{} [{}]: acquiring connection", table.fqn, chunk_label);
    // Dedicated connection (outside the pool, closed on drop): a stalled stream can never
    // leave a half-dead connection in the pool for the next attempt to be handed.
    let mut conn = tokio::time::timeout(
        std::time::Duration::from_secs(90),
        pool.dedicated_connection(),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!(
            "{} [{}]: connect timed out after 90s",
            table.fqn,
            chunk_label
        )
    })??;
    tracing::debug!(
        "{} [{}]: connection acquired, building query",
        table.fqn,
        chunk_label
    );

    // Build SELECT list with CAST overrides for special types
    let select_cols: Vec<String> = table
        .columns
        .iter()
        .map(|c| {
            let expr = select_expr(&c.name, &c.sql_type);
            if matches!(c.sql_type.to_lowercase().as_str(), "sql_variant" | "hierarchyid") {
                tracing::warn!(
                    "{}: column [{}] is {} — exporting as string (data preserved, original type narrowed)",
                    table.fqn, c.name, c.sql_type
                );
            }
            expr.unwrap_or_else(|| format!("[{}]", c.name))
        })
        .collect();

    let query = match &chunk {
        Some(c) => format!(
            "SELECT {} FROM {} WITH (NOLOCK) WHERE {}",
            select_cols.join(", "),
            table.fqn,
            chunk_predicate(c)
        ),
        None => format!(
            "SELECT {} FROM {} WITH (NOLOCK)",
            select_cols.join(", "),
            table.fqn
        ),
    };

    let part_suffix = if chunk.is_some() {
        format!(".part{part_index}")
    } else {
        String::new()
    };
    // The archive path is stable; the on-disk temp file is unique per attempt (see TMP_FILE_SEQ).
    let archive_name = format!("{}{}.parquet", fqn_to_filename(&table.fqn), part_suffix);
    let archive_path = format!("data/{archive_name}");
    let seq = TMP_FILE_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let local_filename = format!(
        "{}{}.a{seq}.parquet",
        fqn_to_filename(&table.fqn),
        part_suffix
    );
    let parquet_path = tmp_dir.join(&local_filename);

    let parquet_path_for_writer = parquet_path.clone();
    let table_for_writer = table.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<tiberius::Row>>(2);
    let writer_handle = tokio::task::spawn_blocking(move || {
        BlockingParquetWriter::new(
            &parquet_path_for_writer,
            &table_for_writer,
            compression_level,
            rx,
        )?
        .run()
    });

    // Send query with a timeout — a hang here (server accepts connection but never
    // returns first TDS packet) is otherwise only caught by the outer wall-clock wrapper.
    let rows_stream =
        tokio::time::timeout(std::time::Duration::from_secs(120), conn.query(&query, &[]))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "{} [{}]: query send timed out after 120s",
                    table.fqn,
                    chunk_label
                )
            })??
            .into_row_stream();
    futures::pin_mut!(rows_stream);
    tracing::debug!(
        "{} [{}]: query sent, streaming rows",
        table.fqn,
        chunk_label
    );

    let chunk_start = std::time::Instant::now();
    let mut last_heartbeat = std::time::Instant::now();
    let mut rows_since_heartbeat: u64 = 0;
    let mut row_batch: Vec<tiberius::Row> = Vec::with_capacity(batch_size);
    let mut rows_read: u64 = 0;
    let mut writer_died = false;
    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(300), rows_stream.next()).await {
            Ok(Some(row_result)) => {
                row_batch.push(row_result?);
                rows_since_heartbeat += 1;
                pb.inc(1);
                if row_batch.len() >= batch_size {
                    tracing::debug!(
                        "{} [{}]: flushing batch of {} rows to Parquet writer",
                        table.fqn,
                        chunk_label,
                        row_batch.len()
                    );
                    let batch = std::mem::take(&mut row_batch);
                    rows_read += batch.len() as u64;
                    // A failed send means the writer thread has died; stop reading and surface
                    // its real error below instead of streaming the rest of the table for nothing.
                    if tx.send(batch).is_err() {
                        writer_died = true;
                        break;
                    }
                }
                // Heartbeat every 30s — shows live rows/sec per chunk
                if last_heartbeat.elapsed().as_secs() >= 30 {
                    let rps = rows_since_heartbeat as f64 / last_heartbeat.elapsed().as_secs_f64();
                    tracing::info!(
                        "{} [{}]: ♥ {:.0} rows/s  (elapsed {:.0}s)",
                        table.fqn,
                        chunk_label,
                        rps,
                        chunk_start.elapsed().as_secs_f64()
                    );
                    rows_since_heartbeat = 0;
                    last_heartbeat = std::time::Instant::now();
                }
            }
            Ok(None) => {
                if !row_batch.is_empty() {
                    rows_read += row_batch.len() as u64;
                    if tx.send(row_batch).is_err() {
                        writer_died = true;
                    }
                }
                break;
            }
            Err(_) => {
                anyhow::bail!(
                    "Row stream for {} timed out after 300s with no data — \
                     network stall or server pause. Retry the export.",
                    table.fqn
                );
            }
        }
    }

    // Close the channel so the writer finishes, then collect its result (its error, if any,
    // is the root cause when `writer_died`).
    drop(tx);
    let rows = writer_handle.await.context("Writer task panicked")??;
    if writer_died || rows != rows_read {
        anyhow::bail!(
            "{} [{}]: row count mismatch — read {} rows from the server, wrote {} to Parquet",
            table.fqn,
            chunk_label,
            rows_read,
            rows
        );
    }
    pb.finish_with_message(format!("{rows} rows"));

    let elapsed = chunk_start.elapsed();
    let rps = rows as f64 / elapsed.as_secs_f64().max(0.001);
    tracing::info!(
        "{} [{}]: ✓ {} rows in {:.1}s ({:.0} rows/s)",
        table.fqn,
        chunk_label,
        rows,
        elapsed.as_secs_f64(),
        rps
    );

    Ok(TableExportStat {
        table_fqn: table.fqn.clone(),
        local_filenames: vec![local_filename],
        archive_paths: vec![archive_path],
        rows,
        has_identity: !table.identity_columns.is_empty(),
        identity_columns: table.identity_columns,
    })
}

/// Plan chunks for a large table. Returns vec of ChunkSpec.
/// - Indexed tables: MIN/MAX range split (O(1), no scan).
/// - Heap tables with a known int column: modulo split — N parallel full scans.
///   SQL Server's scan-sharing (allocation-order reads under NOLOCK) means concurrent
///   heap scans reuse the same physical read I/O; we get N-core decode parallelism
///   on the client for the cost of ~1 physical scan on the server.
///   Exception: LOB-heavy tables (nvarchar(max)/varchar(max)/text) have off-row data
///   and forwarded records that break scan-sharing. Halve their parallelism to avoid
///   contended random I/O being worse than fewer synchronized sequential scans.
///
/// Returns empty vec if no chunk_key or estimated rows < threshold.
async fn plan_chunks(
    pool: &crate::connection::DbPool,
    table: &TableInfo,
    n: usize,
    threshold: u64,
) -> Result<Vec<ChunkSpec>> {
    // `pk` is the raw column name; `qpk` is the bracket-quoted form for SQL text.
    let pk = match &table.chunk_key {
        Some(k) if n > 1 => k.clone(),
        _ => return Ok(vec![]),
    };
    let qpk = quote_ident(&pk);

    // Detect LOB columns on unindexed heaps. LOB tables on heaps use range chunking
    // (sequential I/O) instead of modulo chunking (scattered I/O), so they do NOT
    // need reduced parallelism — range chunking already solves the LOB_DATA contention
    // problem. Only modulo-chunked tables (non-LOB heaps) need halving to avoid
    // simultaneous scattered LOB_DATA streams from different modulo residues.
    let has_lob = !table.chunk_key_indexed && has_lob_column(table);

    // Heap LOB tables are range-chunked with NTILE; more, smaller chunks keep every
    // stream busy to the end instead of leaving a 1-2 stream tail.
    let n = if has_lob {
        std::env::var("SQLRUSTLER_LOB_CHUNKS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&c| c >= 1)
            .unwrap_or(n)
    } else {
        n
    };

    // Fast row-count estimate from partition stats (no table scan)
    let mut conn = pool.get().await?;
    let est_sql = format!(
        "SELECT SUM(p.rows) FROM sys.partitions p \
         JOIN sys.tables t ON t.object_id = p.object_id \
         JOIN sys.schemas s ON s.schema_id = t.schema_id \
         WHERE s.name = '{}' AND t.name = '{}' AND p.index_id IN (0,1)",
        escape_sql_literal(&table.schema),
        escape_sql_literal(&table.name)
    );
    let est_rows: i64 = conn
        .query(&est_sql, &[])
        .await?
        .into_results()
        .await?
        .into_iter()
        .flatten()
        .next()
        .and_then(|r| r.get::<i64, _>(0))
        .unwrap_or(0);

    if (est_rows as u64) < threshold {
        return Ok(vec![]);
    }

    if !table.chunk_key_indexed {
        if has_lob {
            // LOB heap — use row-equalized range chunking via NTILE.
            // NTILE sorts only the 4-byte Id column (IN_ROW_DATA only — never follows
            // LOB pointers) and returns exact row-count-equal boundaries. This is far
            // cheaper than scanning LOB values and correctly handles Id gaps from
            // deletions (e.g. Posts has 67% of rows in the first 25% of the Id range).
            // Range chunks read physically contiguous slices of both IN_ROW_DATA and
            // LOB_DATA, converting scattered to sequential I/O.
            let ntile_sql = format!(
                "WITH b AS (SELECT {pk}, NTILE({n}) OVER (ORDER BY {pk}) AS bk FROM {fqn} WITH (NOLOCK)) \
                 SELECT bk, CAST(MIN({pk}) AS BIGINT), CAST(MAX({pk}) AS BIGINT), CAST(COUNT(*) AS BIGINT) FROM b GROUP BY bk ORDER BY bk",
                pk = qpk, n = n, fqn = table.fqn
            );
            let ntile_rows = conn.query(&ntile_sql, &[]).await?.into_results().await?;
            let mut boundaries: Vec<(i64, i64, i64)> = Vec::new();
            for r in ntile_rows.into_iter().flatten() {
                // A bucket we cannot decode must fail planning: silently dropping it would
                // leave a hole in the key space and lose rows.
                let (lo, hi, cnt) =
                    match (r.get::<i64, _>(1), r.get::<i64, _>(2), r.get::<i64, _>(3)) {
                        (Some(lo), Some(hi), Some(cnt)) => (lo, hi, cnt),
                        _ => anyhow::bail!(
                            "{}: could not decode NTILE bucket boundaries for [{}]",
                            table.fqn,
                            pk
                        ),
                    };
                boundaries.push((lo, hi, cnt));
            }

            if boundaries.is_empty() {
                return Ok(vec![]);
            }

            // Trim duplicate keys that straddle a bucket edge so no row is exported twice.
            let ranges = disjoint_bucket_ranges(
                &boundaries
                    .iter()
                    .map(|(lo, hi, _)| (*lo, *hi))
                    .collect::<Vec<_>>(),
            );
            let chunks: Vec<ChunkSpec> = ranges
                .into_iter()
                .map(|(lo, hi)| ChunkSpec::Range {
                    pk: pk.clone(),
                    lo,
                    hi,
                })
                .collect();
            let row_counts: Vec<i64> = boundaries.iter().map(|(_, _, cnt)| *cnt).collect();
            tracing::info!(
                "{}: {} row-equalized range chunks on [{}] ({} rows, LOB heap — row counts: {:?})",
                table.fqn,
                n,
                pk,
                est_rows,
                row_counts
            );
            return Ok(chunks);
        }

        // Non-LOB heap — modulo split. Scan-sharing works well here.
        let chunks: Vec<ChunkSpec> = (0..n)
            .map(|i| ChunkSpec::Modulo {
                pk: pk.clone(),
                divisor: n,
                remainder: i,
            })
            .collect();
        tracing::info!(
            "{}: {} modulo chunks on [{}] ({} rows, heap)",
            table.fqn,
            n,
            pk,
            est_rows
        );
        return Ok(chunks);
    }

    // Indexed table — MIN/MAX arithmetic range split: O(1) vs NTILE's O(n log n).
    let minmax_sql = format!(
        "SELECT CAST(MIN({pk}) AS BIGINT), CAST(MAX({pk}) AS BIGINT) FROM {fqn} WITH (NOLOCK)",
        pk = qpk,
        fqn = table.fqn
    );
    let minmax_rows = conn.query(&minmax_sql, &[]).await?.into_results().await?;
    let (global_lo, global_hi) = minmax_rows
        .into_iter()
        .flatten()
        .next()
        .and_then(|r| Some((r.get::<i64, _>(0)?, r.get::<i64, _>(1)?)))
        .unwrap_or((0, 0));

    if global_lo >= global_hi {
        return Ok(vec![]);
    }

    let chunks: Vec<ChunkSpec> = split_key_range(global_lo, global_hi, n)
        .into_iter()
        .map(|(lo, hi)| ChunkSpec::Range {
            pk: pk.clone(),
            lo,
            hi,
        })
        .collect();

    tracing::info!(
        "{}: splitting into {} chunks via MIN/MAX on [{pk}] (range {global_lo}..{global_hi}, {est_rows} rows)",
        table.fqn, chunks.len(), pk = pk
    );
    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnInfo;

    fn col(name: &str, ty: &str, max_length: i16) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            sql_type: ty.into(),
            precision: 0,
            scale: 0,
            max_length,
            is_nullable: true,
            is_identity: false,
            identity_seed: None,
            identity_increment: None,
        }
    }

    fn table(cols: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            schema: "dbo".into(),
            name: "T".into(),
            fqn: "[dbo].[T]".into(),
            columns: cols,
            identity_columns: vec![],
            chunk_key: Some("Id".into()),
            chunk_key_indexed: false,
            create_ddl: String::new(),
        }
    }

    fn range(lo: i64, hi: i64) -> ChunkSpec {
        ChunkSpec::Range {
            pk: "Id".into(),
            lo,
            hi,
        }
    }
    fn modulo(d: usize, r: usize) -> ChunkSpec {
        ChunkSpec::Modulo {
            pk: "Id".into(),
            divisor: d,
            remainder: r,
        }
    }

    /// Chunks must tile lo..=hi exactly: first starts at lo, last ends at hi, each starts at prev_end+1.
    fn assert_tiles(ranges: &[(i64, i64)], lo: i64, hi: i64) {
        assert!(!ranges.is_empty());
        assert_eq!(ranges[0].0, lo);
        assert_eq!(ranges.last().unwrap().1, hi);
        for w in ranges.windows(2) {
            assert_eq!(
                w[0].1 as i128 + 1,
                w[1].0 as i128,
                "gap or overlap between {:?} and {:?}",
                w[0],
                w[1]
            );
        }
        for (a, b) in ranges {
            assert!(a <= b);
        }
    }

    // ── split_key_range ───────────────────────────────────────────────────────

    #[test]
    fn split_exact_division() {
        let r = split_key_range(1, 100, 4);
        assert_eq!(r, vec![(1, 25), (26, 50), (51, 75), (76, 100)]);
    }

    #[test]
    fn split_uneven_last_chunk_inclusive() {
        let r = split_key_range(1, 10, 3);
        assert_eq!(r, vec![(1, 4), (5, 8), (9, 10)]);
        assert_tiles(&r, 1, 10);
    }

    #[test]
    fn split_more_chunks_than_keys_drops_empty() {
        let r = split_key_range(5, 7, 10);
        assert_eq!(r, vec![(5, 5), (6, 6), (7, 7)]);
    }

    #[test]
    fn split_negative_and_zero_start() {
        let r = split_key_range(-10, 10, 4);
        assert_tiles(&r, -10, 10);
    }

    #[test]
    fn split_extreme_bounds_no_overflow() {
        let r = split_key_range(i64::MIN, i64::MAX, 8);
        assert_tiles(&r, i64::MIN, i64::MAX);
        assert_eq!(r.len(), 8);
    }

    #[test]
    fn split_degenerate_inputs() {
        assert!(split_key_range(10, 1, 4).is_empty());
        assert!(split_key_range(1, 10, 0).is_empty());
        assert_eq!(split_key_range(7, 7, 4), vec![(7, 7)]);
    }

    #[test]
    fn split_always_tiles_for_many_shapes() {
        for n in 1..=17usize {
            for (lo, hi) in [(0i64, 0i64), (1, 1), (1, 1000), (-500, 499), (3, 1_000_003)] {
                let r = split_key_range(lo, hi, n);
                assert_tiles(&r, lo, hi);
                assert!(r.len() <= n);
            }
        }
    }

    // ── disjoint_bucket_ranges ────────────────────────────────────────────────

    #[test]
    fn buckets_already_disjoint_are_unchanged() {
        let b = [(1, 10), (11, 20), (25, 30)];
        assert_eq!(disjoint_bucket_ranges(&b), b.to_vec());
    }

    #[test]
    fn buckets_with_duplicate_key_on_edge_are_trimmed() {
        // key 10 appears in both buckets 1 and 2 (non-unique key): bucket 2 must start at 11.
        let r = disjoint_bucket_ranges(&[(1, 10), (10, 20), (20, 30)]);
        assert_eq!(r, vec![(1, 10), (11, 20), (21, 30)]);
    }

    #[test]
    fn bucket_fully_inside_previous_is_dropped() {
        // one hot key spans three buckets
        let r = disjoint_bucket_ranges(&[(5, 5), (5, 5), (5, 9)]);
        assert_eq!(r, vec![(5, 5), (6, 9)]);
    }

    #[test]
    fn buckets_at_i64_max_do_not_overflow() {
        let r = disjoint_bucket_ranges(&[(1, i64::MAX), (i64::MAX, i64::MAX)]);
        assert_eq!(r, vec![(1, i64::MAX)]);
    }

    // ── chunk_predicate / quoting ─────────────────────────────────────────────

    #[test]
    fn range_predicate_text() {
        assert_eq!(chunk_predicate(&range(1, 9)), "[Id] BETWEEN 1 AND 9");
    }

    #[test]
    fn modulo_predicate_uses_abs_so_negative_keys_are_covered() {
        assert_eq!(chunk_predicate(&modulo(4, 3)), "ABS([Id] % 4) = 3");
    }

    /// Model T-SQL semantics (remainder takes the dividend's sign) and prove ABS(k % d)
    /// puts every key, negative included, in exactly one of the d modulo chunks.
    #[test]
    fn modulo_chunks_partition_all_keys_incl_negative() {
        let d = 5i64;
        for k in -23i64..=23 {
            let tsql_rem = k % d; // Rust % has the same sign rule as T-SQL %
            let matches = (0..d).filter(|r| tsql_rem.abs() == *r).count();
            assert_eq!(matches, 1, "key {k} must match exactly one chunk");
        }
    }

    #[test]
    fn identifier_quoting_escapes_closing_bracket() {
        assert_eq!(quote_ident("Id"), "[Id]");
        assert_eq!(quote_ident("we]ird"), "[we]]ird]");
        assert_eq!(
            chunk_predicate(&ChunkSpec::Range {
                pk: "a]b".into(),
                lo: 0,
                hi: 1
            }),
            "[a]]b] BETWEEN 0 AND 1"
        );
    }

    #[test]
    fn sql_literal_escaping() {
        assert_eq!(escape_sql_literal("O'Brien"), "O''Brien");
    }

    // ── LOB classification ────────────────────────────────────────────────────

    #[test]
    fn lob_detection() {
        assert!(has_lob_column(&table(vec![
            col("Id", "int", 4),
            col("Body", "nvarchar", -1)
        ])));
        assert!(has_lob_column(&table(vec![col("X", "text", 16)])));
        assert!(has_lob_column(&table(vec![col("X", "IMAGE", 16)])));
        assert!(!has_lob_column(&table(vec![
            col("Id", "int", 4),
            col("Name", "nvarchar", 100)
        ])));
    }

    #[test]
    fn lob_range_table_requires_chunks_lob_and_range() {
        let lob = table(vec![col("Body", "nvarchar", -1)]);
        let plain = table(vec![col("Name", "nvarchar", 50)]);
        assert!(is_lob_range_table(&lob, &[range(1, 2)]));
        assert!(!is_lob_range_table(&lob, &[]));
        assert!(!is_lob_range_table(&lob, &[modulo(4, 0)]));
        assert!(!is_lob_range_table(&plain, &[range(1, 2)]));
    }

    // ── retry / aggregation helpers ───────────────────────────────────────────

    #[test]
    fn retry_delay_is_linear() {
        assert_eq!(retry_delay_secs(5, 1), 5);
        assert_eq!(retry_delay_secs(5, 4), 20);
        assert_eq!(retry_delay_secs(10, 2), 20);
    }

    fn stat(rows: u64, part: usize) -> TableExportStat {
        TableExportStat {
            table_fqn: "[dbo].[T]".into(),
            local_filenames: vec![format!("dbo_T.part{part}.a0.parquet")],
            archive_paths: vec![format!("data/dbo_T.part{part}.parquet")],
            rows,
            has_identity: false,
            identity_columns: vec![],
        }
    }

    #[test]
    fn merge_parts_sums_rows_and_keeps_order() {
        let t = table(vec![col("Id", "int", 4)]);
        let m = merge_parts(&t, vec![stat(10, 0), stat(20, 1), stat(5, 2)]);
        assert_eq!(m.rows, 35);
        assert_eq!(
            m.archive_paths,
            vec![
                "data/dbo_T.part0.parquet",
                "data/dbo_T.part1.parquet",
                "data/dbo_T.part2.parquet"
            ]
        );
        assert_eq!(m.local_filenames.len(), 3);
    }

    #[test]
    fn all_tables_exported_check() {
        assert!(check_all_tables_exported(3, 3).is_ok());
        assert!(check_all_tables_exported(3, 2).is_err());
    }

    #[test]
    fn filename_collision_detected() {
        let ok = vec!["[dbo].[A]".to_string(), "[dbo].[B]".to_string()];
        assert!(find_filename_collision(&ok).is_none());
        let bad = vec!["[a_b].[c]".to_string(), "[a].[b_c]".to_string()];
        assert_eq!(
            find_filename_collision(&bad),
            Some(("[a_b].[c]".into(), "[a].[b_c]".into()))
        );
    }
}
