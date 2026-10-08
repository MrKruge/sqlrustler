use anyhow::{Context, Result, bail};
use arrow::array::*;
use arrow::datatypes::DataType;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use regex::Regex;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::archive::ArchiveReader;
use crate::cli::ImportArgs;
use crate::connection::build_pool;
use crate::schema::POST_DATA_MARKER;

pub async fn run(args: ImportArgs) -> Result<()> {
    let start = std::time::Instant::now();

    // 1. Open and read archive
    tracing::info!("Opening archive: {}", args.input.display());
    let archive = ArchiveReader::open(&args.input)?;
    let manifest = archive.read_manifest()?;
    let schema_sql = archive.read_schema_sql()?;

    tracing::info!(
        "Archive: {} tables, {} total rows (format v{})",
        manifest.tables.len(),
        manifest.total_rows,
        manifest.format_version,
    );

    // 2. Split schema at POST_DATA marker
    let (pre_data_ddl, post_data_ddl) = split_schema_sql(&schema_sql);

    // 3. Drop and recreate database if requested
    if args.drop_existing {
        drop_and_recreate_database(&args).await?;
    }

    // 4. Build connection pool
    let pool_size = (args.parallel + 2) as u32;
    let pool = build_pool(&args.conn, pool_size).await?;

    // 5. Execute pre-data DDL (CREATE TABLE etc.)
    if !args.data_only {
        tracing::info!("Executing schema DDL");
        execute_batched_sql(&pool, pre_data_ddl).await.context("Failed to execute schema DDL")?;
    }

    // 6. Import table data in parallel
    let semaphore = Arc::new(Semaphore::new(args.parallel));
    let mut handles = Vec::new();
    let mut total_rows_imported = 0u64;

    for entry in &manifest.tables {
        if entry.row_count == 0 {
            continue;
        }

        let parquet_bytes = match archive.read_parquet(&entry.file_name) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("Skipping {}: {}", entry.table_fqn, e);
                continue;
            }
        };

        let permit = semaphore.clone().acquire_owned().await?;
        let pool = pool.clone();
        let table_fqn = entry.table_fqn.clone();
        let batch_size = args.batch_size;
        let has_identity = entry.has_identity;

        let handle = tokio::spawn(async move {
            let _permit = permit;
            import_table(pool, table_fqn, parquet_bytes, batch_size, has_identity).await
        });
        handles.push(handle);
    }

    for handle in handles {
        let rows = handle.await??;
        total_rows_imported += rows;
    }

    // 7. Execute post-data DDL (FK constraints)
    if !args.data_only && !post_data_ddl.trim().is_empty() {
        tracing::info!("Applying FK constraints and indexes");
        execute_batched_sql(&pool, post_data_ddl)
            .await
            .context("Failed to apply post-data constraints")?;
    }

    let elapsed = start.elapsed();
    tracing::info!(
        "Import complete: {} rows in {:.1}s",
        total_rows_imported,
        elapsed.as_secs_f64()
    );

    Ok(())
}

/// Validate a bracketed FQN from the archive manifest before interpolating into SQL.
/// Accepts only `[schema].[table]` where schema and table are word characters + spaces.
/// Rejects anything that could be used to inject SQL.
fn validate_fqn(fqn: &str) -> Result<()> {
    // Lazy static pattern: compile once
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"^\[[\w ]+\]\.\[[\w ]+\]$").unwrap());
    if re.is_match(fqn) {
        Ok(())
    } else {
        bail!("Untrusted table FQN in archive manifest: {fqn:?} — import aborted")
    }
}

/// Validate a database name before interpolating into SQL (used with --drop-existing).
/// Accepts only alphanumeric characters and underscores.
fn validate_database_name(name: &str) -> Result<()> {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"^[\w]+$").unwrap());
    if re.is_match(name) {
        Ok(())
    } else {
        bail!("Invalid database name: {name:?} — only word characters (a-z, 0-9, _) are allowed")
    }
}

async fn import_table(
    pool: crate::connection::DbPool,
    table_fqn: String,
    parquet_bytes: Vec<u8>,
    batch_size: usize,
    has_identity: bool,
) -> Result<u64> {
    // TODO(bulk-insert): upgrade to tiberius bulk_insert (BCP protocol) for 3-5x faster import.
    // Current: batched INSERT INTO ... VALUES (...) with max 1000 rows per statement.

    // Validate the FQN from the archive — it will be interpolated directly into SQL.
    validate_fqn(&table_fqn)?;

    let mut conn = pool.get().await?;

    // Enable IDENTITY_INSERT if the table has identity columns.
    // P5: each task has its own connection from the pool, so session-scoped SET is safe.
    if has_identity {
        conn.execute(
            &format!("SET IDENTITY_INSERT {table_fqn} ON"),
            &[],
        )
        .await
        .with_context(|| format!("Failed to enable IDENTITY_INSERT for {table_fqn}"))?;
    }

    // Run the actual data load. If it fails, we still need to turn IDENTITY_INSERT off
    // so the connection is usable when returned to the pool.
    let result = import_table_data(&mut conn, &table_fqn, parquet_bytes, batch_size).await;

    // Always disable IDENTITY_INSERT — even on error — to avoid poisoning the connection.
    if has_identity {
        if let Err(e) = conn
            .execute(&format!("SET IDENTITY_INSERT {table_fqn} OFF"), &[])
            .await
        {
            // Log but don't mask the original error
            tracing::warn!("Failed to disable IDENTITY_INSERT for {table_fqn}: {e}");
        }
    }

    let total_rows = result?;
    tracing::debug!("{table_fqn}: {total_rows} rows imported");
    Ok(total_rows)
}

async fn import_table_data(
    conn: &mut crate::connection::TiberiusClient,
    table_fqn: &str,
    parquet_bytes: Vec<u8>,
    batch_size: usize,
) -> Result<u64> {
    let mut total_rows = 0u64;

    let parquet_bytes_obj = Bytes::from(parquet_bytes);
    let reader = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes_obj)
        .context("Failed to open Parquet reader")?
        .with_batch_size(batch_size)
        .build()
        .context("Failed to build Parquet reader")?;

    for batch_result in reader {
        let batch = batch_result.context("Failed to read RecordBatch from Parquet")?;
        let rows = batch.num_rows();

        // INSERT in chunks of at most 1000 rows (SQL Server limit per VALUES clause)
        for chunk_start in (0..rows).step_by(1000) {
            let chunk_end = (chunk_start + 1000).min(rows);
            let chunk = batch.slice(chunk_start, chunk_end - chunk_start);
            insert_batch(conn, table_fqn, &chunk).await?;
        }

        total_rows += rows as u64;
    }

    Ok(total_rows)
}

/// Build and execute a multi-row INSERT statement for one RecordBatch chunk.
/// Uses parameterized approach: converts Arrow arrays back to SQL literal values.
async fn insert_batch(
    conn: &mut crate::connection::TiberiusClient,
    table_fqn: &str,
    batch: &arrow::record_batch::RecordBatch,
) -> Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }

    let schema = batch.schema();
    let col_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("[{}]", f.name()))
        .collect();

    let mut row_values: Vec<String> = Vec::with_capacity(batch.num_rows());

    for row_idx in 0..batch.num_rows() {
        let mut col_vals: Vec<String> = Vec::with_capacity(batch.num_columns());

        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx);
            let val = arrow_value_to_sql(col, row_idx);
            col_vals.push(val);
        }

        row_values.push(format!("({})", col_vals.join(", ")));
    }

    let sql = format!(
        "INSERT INTO {table_fqn} ({}) VALUES {}",
        col_names.join(", "),
        row_values.join(",\n")
    );

    conn.execute(&sql, &[])
        .await
        .with_context(|| format!("INSERT failed for {table_fqn}"))?;

    Ok(())
}

/// Convert a single cell from an Arrow array to a SQL literal string.
/// NULL-safe: returns "NULL" for nulls.
fn arrow_value_to_sql(col: &dyn arrow::array::Array, row_idx: usize) -> String {
    if col.is_null(row_idx) {
        return "NULL".to_string();
    }

    match col.data_type() {
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            if arr.value(row_idx) { "1".to_string() } else { "0".to_string() }
        }
        DataType::Int8 => col.as_any().downcast_ref::<Int8Array>().unwrap().value(row_idx).to_string(),
        DataType::UInt8 => col.as_any().downcast_ref::<UInt8Array>().unwrap().value(row_idx).to_string(),
        DataType::Int16 => col.as_any().downcast_ref::<Int16Array>().unwrap().value(row_idx).to_string(),
        DataType::Int32 => col.as_any().downcast_ref::<Int32Array>().unwrap().value(row_idx).to_string(),
        DataType::Int64 => col.as_any().downcast_ref::<Int64Array>().unwrap().value(row_idx).to_string(),
        DataType::Float32 => col.as_any().downcast_ref::<Float32Array>().unwrap().value(row_idx).to_string(),
        DataType::Float64 => col.as_any().downcast_ref::<Float64Array>().unwrap().value(row_idx).to_string(),
        DataType::Decimal128(_, scale) => {
            let arr = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw = arr.value(row_idx);
            let s = *scale as u32;
            if s == 0 {
                raw.to_string()
            } else {
                let divisor = 10i128.pow(s);
                // Use abs(raw) to split integer and fractional parts, then
                // apply the sign prefix separately — prevents sign loss when
                // abs(raw) < divisor (e.g. raw=-5, scale=2 → "-0.05").
                let sign = if raw < 0 { "-" } else { "" };
                let abs_raw = raw.unsigned_abs();
                let abs_div = divisor.unsigned_abs();
                let int_part = abs_raw / abs_div;
                let frac_part = abs_raw % abs_div;
                format!("{sign}{}.{:0>width$}", int_part, frac_part, width = s as usize)
            }
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            let s = arr.value(row_idx).replace('\'', "''"); // escape single quotes
            format!("N'{s}'")
        }
        DataType::Binary | DataType::FixedSizeBinary(_) => {
            let bytes: &[u8] = match col.data_type() {
                DataType::Binary => {
                    col.as_any().downcast_ref::<BinaryArray>().unwrap().value(row_idx)
                }
                DataType::FixedSizeBinary(_) => {
                    col.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap().value(row_idx)
                }
                _ => unreachable!(),
            };
            let hex: String = bytes.iter().map(|b| format!("{:02X}", b)).collect();
            format!("0x{hex}")
        }
        DataType::Timestamp(_, _) => {
            let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            let micros = arr.value(row_idx);
            let secs = micros / 1_000_000;
            let frac = (micros % 1_000_000).abs();
            // Format as ISO 8601 with explicit UTC suffix so SQL Server knows the timezone.
            // All timestamps are stored as UTC in Parquet (datetimeoffset converted to UTC on export).
            let dt = chrono::DateTime::from_timestamp(secs, (frac * 1000) as u32)
                .unwrap_or_default();
            format!("'{}'", dt.format("%Y-%m-%dT%H:%M:%S%.6fZ"))
        }
        DataType::Date32 => {
            let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row_idx);
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = epoch + chrono::Duration::days(days as i64);
            format!("'{}'", date.format("%Y-%m-%d"))
        }
        DataType::Time64(_) => {
            let arr = col.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap();
            let micros = arr.value(row_idx);
            let h = micros / 3_600_000_000;
            let m = (micros % 3_600_000_000) / 60_000_000;
            let s = (micros % 60_000_000) / 1_000_000;
            let us = micros % 1_000_000;
            format!("'{h:02}:{m:02}:{s:02}.{us:06}'")
        }
        _ => "NULL".to_string(), // Safe fallback for unknown types
    }
}

/// Split schema.sql into pre-data and post-data sections at the POST_DATA marker.
fn split_schema_sql(sql: &str) -> (&str, &str) {
    if let Some(pos) = sql.find(POST_DATA_MARKER) {
        (&sql[..pos], &sql[pos + POST_DATA_MARKER.len()..])
    } else {
        (sql, "")
    }
}

/// Execute a SQL script that may contain GO batch separators.
async fn execute_batched_sql(pool: &crate::connection::DbPool, sql: &str) -> Result<()> {
    let mut conn = pool.get().await?;
    let batch_re = regex::Regex::new(r"(?mi)^\s*GO\s*$").unwrap();

    for batch in batch_re.split(sql) {
        let trimmed = batch.trim();
        if !trimmed.is_empty() {
            conn.execute(trimmed, &[])
                .await
                .with_context(|| format!("SQL batch failed:\n{trimmed}"))?;
        }
    }
    Ok(())
}

async fn drop_and_recreate_database(args: &ImportArgs) -> Result<()> {
    let db = &args.conn.database;
    // Validate before interpolating into SQL — the database name comes from user input.
    validate_database_name(db)?;

    let mut master_args = args.conn.clone();
    master_args.database = "master".to_string();
    let master_pool = build_pool(&master_args, 1).await?;
    let mut conn = master_pool.get().await?;

    tracing::info!("Dropping database [{db}] if it exists");

    conn.execute(
        &format!(
            "IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'{db}') \
             BEGIN \
               ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; \
               DROP DATABASE [{db}] \
             END"
        ),
        &[],
    )
    .await
    .context("Failed to drop existing database")?;

    conn.execute(&format!("CREATE DATABASE [{db}]"), &[])
        .await
        .context("Failed to create database")?;

    tracing::info!("Database [{db}] recreated");
    Ok(())
}
