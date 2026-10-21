use anyhow::{Context, Result};
use futures::StreamExt;
use indicatif::MultiProgress;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::archive::{fqn_to_filename, parquet_archive_path, ArchiveWriter, Manifest, ManifestTable};
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
    Modulo { pk: String, divisor: usize, remainder: usize },
}

pub async fn run(mut args: ExportArgs) -> Result<()> {
    let start = std::time::Instant::now();

    let (parallel, batch_size) = args.effective_parallelism();
    tracing::info!(
        "Export pressure: {} parallel tables, {} rows/batch{}",
        parallel, batch_size,
        args.pressure.as_deref().map(|p| format!(" (--pressure {p})")).unwrap_or_default()
    );

    // 1. Build connection pool.
    // Large tables each get table_parallelism concurrent chunk connections.
    // Small tables share a separate semaphore of size `parallel`.
    // Pool must be large enough for both to run simultaneously without blocking.
    let table_parallelism = args.effective_table_parallelism().max(1);
    let pool_size = (24 + parallel + 8) as u32; // 24 = large_sem cap + small headroom
    let pool = build_pool(&args.conn, pool_size).await?;

    // 2. Extract schema
    tracing::info!("Extracting schema from {}/{}", args.conn.host, args.conn.database);
    let bundle = schema::extract(&pool, &args.exclude_tables).await?;
    tracing::info!("Found {} tables", bundle.tables.len());

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
        Some(monitor::start(
            args.conn.host.clone(),
            args.conn.database.clone(),
            args.azure_resource_group.clone(),
            5,
        ).await)
    } else {
        None
    };

    // 5. Export table data in parallel (skip if schema_only)
    let mut table_stats: Vec<TableExportStat> = Vec::new();

    if !args.schema_only {
        let multi = Arc::new(MultiProgress::new());
        let mut handles: Vec<tokio::task::JoinHandle<Result<Vec<TableExportStat>>>> = Vec::new();
        let tp_threshold = args.table_parallelism_threshold;

        // Pre-plan all chunks.
        let mut sorted_tables = bundle.tables.clone();
        sorted_tables.sort_by_key(|t| if t.chunk_key.is_some() { 0usize } else { 1usize });

        let chunk_plans: Vec<Vec<ChunkSpec>> = {
            let futs: Vec<_> = sorted_tables.iter().map(|table| {
                let pool = pool.clone();
                let table = table.clone();
                async move { plan_chunks(&pool, &table, table_parallelism, tp_threshold).await }
            }).collect();
            futures::future::try_join_all(futs).await?
        };

        // Per-table semaphores: each large table gets its own Semaphore(table_parallelism)
        // so its chunks compete only among themselves. A global cap caused starvation —
        // whichever table acquired permits first monopolised all slots, leaving other tables
        // at zero. Azure Monitor confirmed huge headroom (az_cpu peak 11.2%, az_io peak 12.2%)
        // so there is no server-side reason to impose a cross-table cap.
        // lob_sem throttles scatter-I/O LOB chunks (modulo-chunked LOB heaps like Comments)
        // to avoid thrashing the LOB_DATA buffer pool with too many concurrent random reads.
        let n_large = chunk_plans.iter().filter(|c| !c.is_empty()).count().max(1);
        let lob_sem_size = 4usize;
        let lob_sem   = Arc::new(Semaphore::new(lob_sem_size));
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
            .zip(chunk_plans.into_iter())
            .partition(|(table, chunks)| {
                !chunks.is_empty() && {
                    let has_lob = table.columns.iter().any(|c| {
                        c.max_length == -1
                        || matches!(c.sql_type.to_lowercase().as_str(), "text" | "ntext" | "image")
                    });
                    has_lob && chunks.iter().any(|c| matches!(c, ChunkSpec::Range { .. }))
                }
            });

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
            let compression_level = args.compression_level;
            let pb = make_export_bar(&multi, &table.fqn);
            // LOB range tables (e.g. Posts): cap at 2 concurrent chunks.
            // Evidence from v10/v11 runs:
            //   - 1 concurrent range scan: warms to 4,450 rows/s (v10 chunk 1)
            //   - 8 concurrent range scans: thrashes to ~100 rows/s each (v11 b5q64tdvi)
            // The LOB_DATA buffer pool can sustain ~2 sequential scan fronts before
            // cache churn destroys throughput. lob_scatter_sem handles modulo LOB tables.
            let is_lob_range_table = !chunks.is_empty() && {
                let has_lob = table.columns.iter().any(|c| {
                    c.max_length == -1
                    || matches!(c.sql_type.to_lowercase().as_str(), "text" | "ntext" | "image")
                });
                has_lob && chunks.iter().any(|c| matches!(c, ChunkSpec::Range { .. }))
            };
            let table_sem_size = if is_lob_range_table { 2 } else { table_parallelism };
            let table_sem = Arc::new(tokio::sync::Semaphore::new(table_sem_size));
            let lob_sem = lob_sem.clone();
            let small_sem = small_sem.clone();

            let handle = tokio::spawn(async move {
                const MAX_RETRIES: usize = 5;
                if chunks.is_empty() {
                    // Small table — single connection, uses small_sem.
                    tracing::debug!("{}: waiting for small_sem permit", table.fqn);
                    let mut last_err = None;
                    let mut result = None;
                    for attempt in 0..MAX_RETRIES {
                        if attempt > 0 {
                            let delay = 10 * attempt as u64;
                            tracing::warn!(
                                "{}: stream timed out, retry {}/{} in {}s",
                                table.fqn, attempt, MAX_RETRIES - 1, delay
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                            pb.reset();
                        }
                        let _permit = small_sem.clone().acquire_owned().await?;
                        let chunk_timeout = std::time::Duration::from_secs(300);
                        match tokio::time::timeout(chunk_timeout, export_table(
                            pool.clone(), table.clone(), tmp_path.clone(),
                            batch_size, compression_level, None, 0, pb.clone()
                        )).await {
                            Ok(Ok(stat)) => { result = Some(stat); break; }
                            Ok(Err(e))   => { last_err = Some(e); }
                            Err(_)       => { last_err = Some(anyhow::anyhow!(
                                "{}: wall-clock timeout after {}s — server stalled",
                                table.fqn, chunk_timeout.as_secs()
                            )); }
                        }
                    }
                    let stat = result.ok_or_else(|| last_err.unwrap())?;
                    Ok(vec![stat])
                } else {
                    // Large table — fan out chunks.
                    // Each chunk acquires a slot from this table's own per-table semaphore
                    // (table_sem) so no table can starve another. Modulo-chunked LOB heaps
                    // (scattered random I/O against LOB_DATA) additionally acquire a slot
                    // from the shared lob_sem to bound total concurrent LOB scatter reads.
                    let has_lob = table.columns.iter().any(|c| {
                        c.max_length == -1
                        || matches!(c.sql_type.to_lowercase().as_str(), "text" | "ntext" | "image")
                    });
                    let n = chunks.len();
                    let mut part_handles = Vec::new();
                    for (i, chunk) in chunks.into_iter().enumerate() {
                        let is_range = matches!(chunk, ChunkSpec::Range { .. });
                        let use_lob_sem = has_lob && !is_range;
                        let sem = table_sem.clone();
                        let lob_sem = lob_sem.clone();
                        let pool = pool.clone();
                        let table = table.clone();
                        let tmp_path = tmp_path.clone();
                        let pb = pb.clone();
                        part_handles.push(tokio::spawn(async move {
                            let mut last_err = None;
                            for attempt in 0..MAX_RETRIES {
                                if attempt > 0 {
                                    // Delay must exceed tiberius command_timeout (120s) so that
                                    // cancelled connections are evicted from the pool before we
                                    // call pool.get() again. Without this, pool.get() is handed
                                    // back zombie connections that are "valid" to bb8 (has_broken
                                    // always returns false) but dead to tiberius.
                                    let delay = 130 + 30 * (attempt as u64 - 1);
                                    tracing::warn!(
                                        "{} chunk {}: timed out, retry {}/{} in {}s",
                                        table.fqn, i, attempt, MAX_RETRIES - 1, delay
                                    );
                                    tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                                    pb.reset();
                                }
                                tracing::debug!("{} chunk {}: waiting for table_sem permit", table.fqn, i);
                                let _permit = sem.clone().acquire_owned().await?;
                                // Scatter-I/O LOB chunks also hold a lob_sem slot to bound
                                // concurrent random LOB_DATA reads across all tables.
                                let _lob_permit = if use_lob_sem {
                                    Some(lob_sem.clone().acquire_owned().await?)
                                } else {
                                    None
                                };
                                tracing::debug!("{} chunk {}: permit(s) acquired, starting export", table.fqn, i);
                                // LOB columns (nvarchar(max) etc.) stream at ~1k rows/s — a 932k-row
                                // chunk needs ~930s. Non-LOB tables are fast enough for 300s.
                                let has_lob = table.columns.iter().any(|c| {
                                    c.max_length == -1
                                    || matches!(c.sql_type.to_lowercase().as_str(), "text" | "ntext" | "image")
                                });
                                let chunk_timeout = std::time::Duration::from_secs(if has_lob { 1200 } else { 300 });
                                match tokio::time::timeout(chunk_timeout, export_table(
                                    pool.clone(), table.clone(), tmp_path.clone(),
                                    batch_size, compression_level, Some(chunk.clone()), i, pb.clone(),
                                )).await {
                                    Ok(Ok(stat)) => return Ok(stat),
                                    Ok(Err(e))   => last_err = Some(e),
                                    Err(_)       => last_err = Some(anyhow::anyhow!(
                                        "{} chunk {}: wall-clock timeout after {}s — server stalled",
                                        table.fqn, i, chunk_timeout.as_secs()
                                    )),
                                }
                            }
                            tracing::error!("{} chunk {}: all {} retries exhausted — giving up. Last error: {:#}",
                                table.fqn, i, MAX_RETRIES, last_err.as_ref().unwrap());
                            Err(last_err.unwrap())
                        }));
                    }
                    let mut parts = Vec::with_capacity(n);
                    for h in part_handles { parts.push(h.await??); }
                    let total_rows: u64 = parts.iter().map(|p| p.rows).sum();
                    let mut local_filenames = Vec::new();
                    let mut archive_paths = Vec::new();
                    for p in &parts {
                        local_filenames.extend(p.local_filenames.clone());
                        archive_paths.extend(p.archive_paths.clone());
                    }
                    Ok(vec![TableExportStat {
                        table_fqn: table.fqn.clone(),
                        local_filenames,
                        archive_paths,
                        rows: total_rows,
                        has_identity: !table.identity_columns.is_empty(),
                        identity_columns: table.identity_columns.clone(),
                    }])
                }
            });
            handles.push(handle);
        }

        // Await all phase-1 tables before starting phase-2 (LOB-range tables).
        // This gives Posts an uncontested buffer pool — critical for LOB throughput.
        for handle in handles {
            let stats = handle.await??;
            for stat in stats {
                tracing::info!(
                    "  ✓ {} — {} rows ({}/{} tables done)",
                    stat.table_fqn, stat.rows,
                    table_stats.len() + 1, bundle.tables.len()
                );
                table_stats.push(stat);
            }
        }

        // Phase-2: spawn LOB-range tables (Posts) — same spawn logic, now with uncontested buffer pool.
        let mut lob_handles: Vec<tokio::task::JoinHandle<Result<Vec<TableExportStat>>>> = Vec::new();
        for (table, chunks) in lob_range_pairs {
            let pool = pool.clone();
            let table = table.clone();
            let tmp_path = tmp_dir.path().to_path_buf();
            let compression_level = args.compression_level;
            let pb = make_export_bar(&multi, &table.fqn);
            let is_lob_range_table = true; // by construction
            let table_sem_size = 2usize; // proven sweet spot: 2 concurrent LOB range scans
            let table_sem = Arc::new(tokio::sync::Semaphore::new(table_sem_size));
            let lob_sem = lob_sem.clone();
            let small_sem = small_sem.clone();
            let handle = tokio::spawn(async move {
                const MAX_RETRIES: usize = 5;
                let has_lob = true;
                let n = chunks.len();
                let mut part_handles = Vec::new();
                for (i, chunk) in chunks.into_iter().enumerate() {
                    let is_range = matches!(chunk, ChunkSpec::Range { .. });
                    let use_lob_sem = has_lob && !is_range;
                    let sem = table_sem.clone();
                    let lob_sem = lob_sem.clone();
                    let pool = pool.clone();
                    let table = table.clone();
                    let tmp_path = tmp_path.clone();
                    let pb = pb.clone();
                    part_handles.push(tokio::spawn(async move {
                        let mut last_err = None;
                        for attempt in 0..MAX_RETRIES {
                            if attempt > 0 {
                                let delay = 130 + 30 * (attempt as u64 - 1);
                                tracing::warn!("{} chunk {}: timed out, retry {}/{} in {}s",
                                    table.fqn, i, attempt, MAX_RETRIES - 1, delay);
                                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                                pb.reset();
                            }
                            let _permit = sem.clone().acquire_owned().await?;
                            let _lob_permit = if use_lob_sem {
                                Some(lob_sem.clone().acquire_owned().await?)
                            } else { None };
                            let chunk_timeout = std::time::Duration::from_secs(1200);
                            match tokio::time::timeout(chunk_timeout, export_table(
                                pool.clone(), table.clone(), tmp_path.clone(),
                                batch_size, compression_level, Some(chunk.clone()), i, pb.clone(),
                            )).await {
                                Ok(Ok(stat)) => return Ok(stat),
                                Ok(Err(e))   => last_err = Some(e),
                                Err(_)       => last_err = Some(anyhow::anyhow!(
                                    "{} chunk {}: wall-clock timeout after 1200s",
                                    table.fqn, i
                                )),
                            }
                        }
                        tracing::error!("{} chunk {}: all {} retries exhausted: {:#}",
                            table.fqn, i, MAX_RETRIES, last_err.as_ref().unwrap());
                        Err(last_err.unwrap())
                    }));
                }
                let mut parts = Vec::with_capacity(n);
                for h in part_handles { parts.push(h.await??); }
                let total_rows: u64 = parts.iter().map(|p| p.rows).sum();
                let mut local_filenames = Vec::new();
                let mut archive_paths = Vec::new();
                for p in &parts {
                    local_filenames.extend(p.local_filenames.clone());
                    archive_paths.extend(p.archive_paths.clone());
                }
                Ok(vec![TableExportStat {
                    table_fqn: table.fqn.clone(),
                    local_filenames,
                    archive_paths,
                    rows: total_rows,
                    has_identity: !table.identity_columns.is_empty(),
                    identity_columns: table.identity_columns.clone(),
                }])
            });
            lob_handles.push(handle);
        }
        for handle in lob_handles {
            let stats = handle.await??;
            for stat in stats {
                tracing::info!(
                    "  ✓ {} — {} rows ({}/{} tables done)",
                    stat.table_fqn, stat.rows,
                    table_stats.len() + 1, bundle.tables.len()
                );
                table_stats.push(stat);
            }
        }
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
            .chain(
                if args.schema_only {
                    bundle.tables.iter()
                        .map(|t| ManifestTable {
                            table_fqn: t.fqn.clone(),
                            file_names: vec![parquet_archive_path(&t.fqn)],
                            file_name: String::new(),
                            row_count: 0,
                            has_identity: !t.identity_columns.is_empty(),
                            identity_columns: t.identity_columns.clone(),
                        })
                        .collect::<Vec<_>>().into_iter()
                } else {
                    vec![].into_iter()
                },
            )
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
    let mut conn = pool.get().await.context("Failed to get connection for validation")?;
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
                stat.table_fqn, stat.rows, live_count
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
    batch_size: usize,
    compression_level: i32,
    chunk: Option<ChunkSpec>,
    part_index: usize,
    pb: indicatif::ProgressBar,
) -> Result<TableExportStat> {
    let chunk_label = match &chunk {
        Some(ChunkSpec::Modulo { remainder, divisor, .. }) => format!("chunk {remainder}/{divisor}"),
        Some(ChunkSpec::Range { lo, hi, .. }) => format!("range {lo}..{hi}"),
        None => "single".to_string(),
    };
    tracing::debug!("{} [{}]: acquiring connection", table.fqn, chunk_label);
    let mut conn = tokio::time::timeout(
        std::time::Duration::from_secs(90),
        pool.get()
    ).await
    .map_err(|_| anyhow::anyhow!("{} [{}]: pool.get() timed out after 90s", table.fqn, chunk_label))??;
    tracing::debug!("{} [{}]: connection acquired, building query", table.fqn, chunk_label);

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
        Some(ChunkSpec::Range { pk, lo, hi }) => format!(
            "SELECT {} FROM {} WITH (NOLOCK) WHERE [{pk}] BETWEEN {lo} AND {hi}",
            select_cols.join(", "), table.fqn
        ),
        Some(ChunkSpec::Modulo { pk, divisor, remainder }) => format!(
            "SELECT {} FROM {} WITH (NOLOCK) WHERE [{pk}] % {divisor} = {remainder}",
            select_cols.join(", "), table.fqn
        ),
        None => format!("SELECT {} FROM {} WITH (NOLOCK)", select_cols.join(", "), table.fqn),
    };

    let part_suffix = if chunk.is_some() { format!(".part{part_index}") } else { String::new() };
    let local_filename = format!("{}{}.parquet", fqn_to_filename(&table.fqn), part_suffix);
    let parquet_path = tmp_dir.join(&local_filename);
    let archive_path = format!("data/{local_filename}");

    let parquet_path_for_writer = parquet_path.clone();
    let table_for_writer = table.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<tiberius::Row>>(2);
    let writer_handle = tokio::task::spawn_blocking(move || {
        BlockingParquetWriter::new(&parquet_path_for_writer, &table_for_writer, compression_level, rx)?.run()
    });

    // Send query with a timeout — a hang here (server accepts connection but never
    // returns first TDS packet) is otherwise only caught by the outer wall-clock wrapper.
    let rows_stream = tokio::time::timeout(
        std::time::Duration::from_secs(120),
        conn.query(&query, &[])
    ).await
    .map_err(|_| anyhow::anyhow!("{} [{}]: query send timed out after 120s", table.fqn, chunk_label))??
    .into_row_stream();
    futures::pin_mut!(rows_stream);
    tracing::debug!("{} [{}]: query sent, streaming rows", table.fqn, chunk_label);

    let chunk_start = std::time::Instant::now();
    let mut last_heartbeat = std::time::Instant::now();
    let mut rows_since_heartbeat: u64 = 0;
    let mut row_batch: Vec<tiberius::Row> = Vec::with_capacity(batch_size);
    loop {
        match tokio::time::timeout(
            std::time::Duration::from_secs(300),
            rows_stream.next(),
        ).await {
            Ok(Some(row_result)) => {
                row_batch.push(row_result?);
                rows_since_heartbeat += 1;
                pb.inc(1);
                if row_batch.len() >= batch_size {
                    tracing::debug!("{} [{}]: flushing batch of {} rows to Parquet writer",
                        table.fqn, chunk_label, row_batch.len());
                    tx.send(std::mem::take(&mut row_batch)).ok();
                }
                // Heartbeat every 30s — shows live rows/sec per chunk
                if last_heartbeat.elapsed().as_secs() >= 30 {
                    let rps = rows_since_heartbeat as f64 / last_heartbeat.elapsed().as_secs_f64();
                    tracing::info!("{} [{}]: ♥ {:.0} rows/s  (elapsed {:.0}s)",
                        table.fqn, chunk_label, rps, chunk_start.elapsed().as_secs_f64());
                    rows_since_heartbeat = 0;
                    last_heartbeat = std::time::Instant::now();
                }
            }
            Ok(None) => {
                if !row_batch.is_empty() { tx.send(row_batch).ok(); }
                drop(tx);
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

    let rows = writer_handle.await.context("Writer task panicked")??;
    pb.finish_with_message(format!("{rows} rows"));

    let elapsed = chunk_start.elapsed();
    let rps = rows as f64 / elapsed.as_secs_f64().max(0.001);
    tracing::info!("{} [{}]: ✓ {} rows in {:.1}s ({:.0} rows/s)",
        table.fqn, chunk_label, rows, elapsed.as_secs_f64(), rps);

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
/// Returns empty vec if no chunk_key or estimated rows < threshold.
async fn plan_chunks(
    pool: &crate::connection::DbPool,
    table: &TableInfo,
    n: usize,
    threshold: u64,
) -> Result<Vec<ChunkSpec>> {
    let pk = match &table.chunk_key {
        Some(k) if n > 1 => k.clone(),
        _ => return Ok(vec![]),
    };

    // Detect LOB columns on unindexed heaps. LOB tables on heaps use range chunking
    // (sequential I/O) instead of modulo chunking (scattered I/O), so they do NOT
    // need reduced parallelism — range chunking already solves the LOB_DATA contention
    // problem. Only modulo-chunked tables (non-LOB heaps) need halving to avoid
    // simultaneous scattered LOB_DATA streams from different modulo residues.
    let has_lob = !table.chunk_key_indexed && table.columns.iter().any(|c| {
        // max_length == -1 means nvarchar(max)/varchar(max)/varbinary(max)
        // "text"/"ntext"/"image" are legacy LOB types with the same off-row problem
        c.max_length == -1
        || matches!(c.sql_type.to_lowercase().as_str(), "text" | "ntext" | "image")
    });

    // Fast row-count estimate from partition stats (no table scan)
    let mut conn = pool.get().await?;
    let est_sql = format!(
        "SELECT SUM(p.rows) FROM sys.partitions p \
         JOIN sys.tables t ON t.object_id = p.object_id \
         JOIN sys.schemas s ON s.schema_id = t.schema_id \
         WHERE s.name = '{}' AND t.name = '{}' AND p.index_id IN (0,1)",
        table.schema, table.name
    );
    let est_rows: i64 = conn.query(&est_sql, &[]).await?
        .into_results().await?
        .into_iter().flatten().next()
        .and_then(|r| r.get::<i64, _>(0))
        .unwrap_or(0);

    if (est_rows as u64) < threshold { return Ok(vec![]); }

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
                "WITH b AS (SELECT [{pk}], NTILE({n}) OVER (ORDER BY [{pk}]) AS bk FROM {fqn} WITH (NOLOCK)) \
                 SELECT bk, CAST(MIN([{pk}]) AS BIGINT), CAST(MAX([{pk}]) AS BIGINT), CAST(COUNT(*) AS BIGINT) FROM b GROUP BY bk ORDER BY bk",
                pk = pk, n = n, fqn = table.fqn
            );
            let ntile_rows = conn.query(&ntile_sql, &[]).await?.into_results().await?;
            let boundaries: Vec<(i64, i64, i64)> = ntile_rows.into_iter().flatten()
                .filter_map(|r| Some((r.get::<i64, _>(1)?, r.get::<i64, _>(2)?, r.get::<i64, _>(3)?)))
                .collect();

            if boundaries.is_empty() { return Ok(vec![]); }

            let chunks: Vec<ChunkSpec> = boundaries.iter()
                .map(|(lo, hi, _)| ChunkSpec::Range { pk: pk.clone(), lo: *lo, hi: *hi })
                .collect();
            let row_counts: Vec<i64> = boundaries.iter().map(|(_, _, cnt)| *cnt).collect();
            tracing::info!(
                "{}: {} row-equalized range chunks on [{}] ({} rows, LOB heap — row counts: {:?})",
                table.fqn, n, pk, est_rows, row_counts
            );
            return Ok(chunks);
        }

        // Non-LOB heap — modulo split. Scan-sharing works well here.
        let chunks: Vec<ChunkSpec> = (0..n)
            .map(|i| ChunkSpec::Modulo { pk: pk.clone(), divisor: n, remainder: i })
            .collect();
        tracing::info!(
            "{}: {} modulo chunks on [{}] ({} rows, heap)",
            table.fqn, n, pk, est_rows
        );
        return Ok(chunks);
    }

    // Indexed table — MIN/MAX arithmetic range split: O(1) vs NTILE's O(n log n).
    let minmax_sql = format!(
        "SELECT CAST(MIN([{pk}]) AS BIGINT), CAST(MAX([{pk}]) AS BIGINT) FROM {fqn} WITH (NOLOCK)",
        pk = pk, fqn = table.fqn
    );
    let minmax_rows = conn.query(&minmax_sql, &[]).await?.into_results().await?;
    let (global_lo, global_hi) = minmax_rows.into_iter().flatten().next()
        .and_then(|r| Some((r.get::<i64, _>(0)?, r.get::<i64, _>(1)?)))
        .unwrap_or((0, 0));

    if global_lo >= global_hi { return Ok(vec![]); }

    let range = global_hi - global_lo + 1;
    let chunk_size = (range + n as i64 - 1) / n as i64;
    let chunks: Vec<ChunkSpec> = (0..n as i64)
        .map(|i| {
            let lo = global_lo + i * chunk_size;
            let hi = (global_lo + (i + 1) * chunk_size - 1).min(global_hi);
            (lo, hi)
        })
        .filter(|(lo, hi)| lo <= hi)
        .map(|(lo, hi)| ChunkSpec::Range { pk: pk.clone(), lo, hi })
        .collect();

    tracing::info!(
        "{}: splitting into {} chunks via MIN/MAX on [{pk}] (range {global_lo}..{global_hi}, {est_rows} rows)",
        table.fqn, chunks.len(), pk = pk
    );
    Ok(chunks)
}
