use anyhow::Result;
use futures::StreamExt;
use indicatif::MultiProgress;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::archive::{fqn_to_filename, parquet_archive_path, ArchiveWriter, Manifest, ManifestTable};
use crate::cli::ExportArgs;
use crate::connection::build_pool;
use crate::parquet_writer::ParquetTableWriter;
use crate::progress::make_export_bar;
use crate::schema::{self, TableInfo};
use crate::types::select_expr;

pub async fn run(args: ExportArgs) -> Result<()> {
    let start = std::time::Instant::now();

    // 1. Build connection pool: +2 connections for schema extraction + manifest
    let pool_size = (args.parallel + 2) as u32;
    let pool = build_pool(&args.conn, pool_size).await?;

    // 2. Extract schema
    tracing::info!("Extracting schema from {}/{}", args.conn.host, args.conn.database);
    let bundle = schema::extract(&pool, &args.exclude_tables).await?;
    tracing::info!("Found {} tables", bundle.tables.len());

    // 3. Determine output path
    let output_path = args.output.unwrap_or_else(|| {
        let ts = chrono::Utc::now().format("%Y%m%dT%H%M%S");
        format!("{}_{}.sqlrustler", args.conn.database, ts).into()
    });

    // 4. Temp directory for per-table Parquet files
    let tmp_dir = tempfile::tempdir()?;

    // 5. Export table data in parallel (skip if schema_only)
    let mut table_stats: Vec<TableExportStat> = Vec::new();

    if !args.schema_only {
        let semaphore = Arc::new(Semaphore::new(args.parallel));
        let multi = Arc::new(MultiProgress::new());
        let mut handles = Vec::new();

        for table in &bundle.tables {
            let permit = semaphore.clone().acquire_owned().await?;
            let pool = pool.clone();
            let table = table.clone();
            let tmp_path = tmp_dir.path().to_path_buf();
            let batch_size = args.batch_size;
            let pb = make_export_bar(&multi, &table.fqn);

            let handle = tokio::spawn(async move {
                let _permit = permit; // held until task completes → throttles parallelism
                export_table(pool, table, tmp_path, batch_size, pb).await
            });
            handles.push(handle);
        }

        for handle in handles {
            let stat = handle.await??;
            table_stats.push(stat);
        }
    }

    // 6. Assemble archive
    tracing::info!("Writing archive to {}", output_path.display());
    let mut archive = ArchiveWriter::create(&output_path, args.compression_level)?;
    archive.write_schema_sql(&bundle.schema_sql)?;

    let total_rows: u64 = table_stats.iter().map(|s| s.rows).sum();
    let manifest = Manifest {
        sqlrustler_version: env!("CARGO_PKG_VERSION").to_string(),
        format_version: 1,
        created_at: chrono::Utc::now().to_rfc3339(),
        source_host: args.conn.host.clone(),
        source_database: args.conn.database.clone(),
        total_rows,
        schema_only: args.schema_only,
        tables: table_stats
            .iter()
            .map(|s| ManifestTable {
                table_fqn: s.table_fqn.clone(),
                file_name: s.archive_path.clone(),
                row_count: s.rows,
                has_identity: s.has_identity,
                identity_columns: s.identity_columns.clone(),
            })
            .chain(
                // include schema-only entries with 0 rows for --schema-only mode
                if args.schema_only {
                    bundle
                        .tables
                        .iter()
                        .map(|t| ManifestTable {
                            table_fqn: t.fqn.clone(),
                            file_name: parquet_archive_path(&t.fqn),
                            row_count: 0,
                            has_identity: !t.identity_columns.is_empty(),
                            identity_columns: t.identity_columns.clone(),
                        })
                        .collect::<Vec<_>>()
                        .into_iter()
                } else {
                    vec![].into_iter()
                },
            )
            .collect(),
    };
    archive.write_manifest(&manifest)?;

    for stat in &table_stats {
        let parquet_path = tmp_dir.path().join(&stat.local_filename);
        archive.add_parquet_file(&stat.archive_path, &parquet_path)?;
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

    Ok(())
}

struct TableExportStat {
    table_fqn: String,
    local_filename: String,
    archive_path: String,
    rows: u64,
    has_identity: bool,
    identity_columns: Vec<String>,
}

async fn export_table(
    pool: crate::connection::DbPool,
    table: TableInfo,
    tmp_dir: std::path::PathBuf,
    batch_size: usize,
    pb: indicatif::ProgressBar,
) -> Result<TableExportStat> {
    let mut conn = pool.get().await?;

    // Build SELECT list with CAST overrides for special types
    let select_cols: Vec<String> = table
        .columns
        .iter()
        .map(|c| {
            select_expr(&c.name, &c.sql_type).unwrap_or_else(|| format!("[{}]", c.name))
        })
        .collect();

    let query = format!(
        "SELECT {} FROM {} WITH (NOLOCK)",
        select_cols.join(", "),
        table.fqn
    );

    let local_filename = format!("{}.parquet", fqn_to_filename(&table.fqn));
    let parquet_path = tmp_dir.join(&local_filename);
    let archive_path = parquet_archive_path(&table.fqn);

    let mut writer = ParquetTableWriter::new(&parquet_path, &table, batch_size)?;

    // tiberius QueryStream yields QueryItem::{Row, Metadata}; we only want rows
    let rows_stream = conn.query(&query, &[]).await?.into_row_stream();
    futures::pin_mut!(rows_stream);
    while let Some(row_result) = rows_stream.next().await {
        let row = row_result?;
        writer.push_row(row)?;
        pb.inc(1);
    }

    let rows = writer.finish()?;
    pb.finish_with_message(format!("{rows} rows"));

    Ok(TableExportStat {
        table_fqn: table.fqn,
        local_filename,
        archive_path,
        rows,
        has_identity: !table.identity_columns.is_empty(),
        identity_columns: table.identity_columns,
    })
}
