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
pub(crate) fn split_schema_sql(sql: &str) -> (&str, &str) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Int16Builder,
        Int32Builder, Int64Builder, Int8Builder, StringArray, Time64MicrosecondBuilder,
        TimestampMicrosecondBuilder, UInt8Builder,
    };
    use arrow::datatypes::TimeUnit;
    use std::sync::Arc;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn scalar<B: arrow::array::ArrayBuilder>(mut builder: B, append: impl Fn(&mut B)) -> Arc<dyn arrow::array::Array> {
        append(&mut builder);
        Arc::new(builder.finish())
    }

    fn null_array(data_type: arrow::datatypes::DataType) -> Arc<dyn arrow::array::Array> {
        use arrow::array::new_null_array;
        new_null_array(&data_type, 1)
    }

    // ── NULL tests ────────────────────────────────────────────────────────────

    #[test]
    fn null_int32_returns_null() {
        let arr = null_array(arrow::datatypes::DataType::Int32);
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "NULL");
    }

    #[test]
    fn null_utf8_returns_null() {
        let arr = null_array(arrow::datatypes::DataType::Utf8);
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "NULL");
    }

    #[test]
    fn null_decimal128_returns_null() {
        let mut builder = Decimal128Builder::new().with_precision_and_scale(18, 4).unwrap();
        builder.append_null();
        let arr: Arc<dyn arrow::array::Array> = Arc::new(builder.finish());
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "NULL");
    }

    #[test]
    fn null_binary_returns_null() {
        let arr = null_array(arrow::datatypes::DataType::Binary);
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "NULL");
    }

    #[test]
    fn null_boolean_returns_null() {
        let arr = null_array(arrow::datatypes::DataType::Boolean);
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "NULL");
    }

    #[test]
    fn null_timestamp_returns_null() {
        let arr = null_array(arrow::datatypes::DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        ));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "NULL");
    }

    #[test]
    fn null_date32_returns_null() {
        let arr = null_array(arrow::datatypes::DataType::Date32);
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "NULL");
    }

    #[test]
    fn null_time64_returns_null() {
        let arr = null_array(arrow::datatypes::DataType::Time64(TimeUnit::Microsecond));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "NULL");
    }

    // ── Integer types ─────────────────────────────────────────────────────────

    #[test]
    fn int8_produces_number_string() {
        let arr = scalar(Int8Builder::new(), |b| b.append_value(42));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "42");
    }

    #[test]
    fn int16_produces_number_string() {
        let arr = scalar(Int16Builder::new(), |b| b.append_value(1000));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "1000");
    }

    #[test]
    fn int32_produces_number_string() {
        let arr = scalar(Int32Builder::new(), |b| b.append_value(-99999));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "-99999");
    }

    #[test]
    fn int64_produces_number_string() {
        let arr = scalar(Int64Builder::new(), |b| b.append_value(9_876_543_210i64));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "9876543210");
    }

    #[test]
    fn uint8_produces_number_string() {
        let arr = scalar(UInt8Builder::new(), |b| b.append_value(255));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "255");
    }

    // ── Boolean ───────────────────────────────────────────────────────────────

    #[test]
    fn boolean_true_produces_1() {
        let arr = scalar(BooleanBuilder::new(), |b| b.append_value(true));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "1");
    }

    #[test]
    fn boolean_false_produces_0() {
        let arr = scalar(BooleanBuilder::new(), |b| b.append_value(false));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "0");
    }

    // ── Decimal128 ────────────────────────────────────────────────────────────

    fn decimal_array(raw: i128, precision: u8, scale: i8) -> Arc<dyn arrow::array::Array> {
        let mut b = Decimal128Builder::new()
            .with_precision_and_scale(precision, scale)
            .unwrap();
        b.append_value(raw);
        Arc::new(b.finish())
    }

    #[test]
    fn decimal128_scale0_raw_12345() {
        let arr = decimal_array(12345, 10, 0);
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "12345");
    }

    #[test]
    fn decimal128_scale2_positive() {
        // raw=12345, scale=2 → "123.45"
        let arr = decimal_array(12345, 10, 2);
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "123.45");
    }

    #[test]
    fn decimal128_scale2_negative() {
        // raw=-12345, scale=2 → "-123.45"  (the sign bug fix)
        let arr = decimal_array(-12345, 10, 2);
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "-123.45");
    }

    #[test]
    fn decimal128_scale2_negative_near_zero() {
        // raw=-5, scale=2 → "-0.05"  (the near-zero sign bug fix)
        let arr = decimal_array(-5, 10, 2);
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "-0.05");
    }

    #[test]
    fn decimal128_scale2_positive_near_zero() {
        // raw=5, scale=2 → "0.05"
        let arr = decimal_array(5, 10, 2);
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "0.05");
    }

    #[test]
    fn decimal128_scale4_large() {
        // raw=12345678, scale=4 → "1234.5678"
        let arr = decimal_array(12345678, 18, 4);
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "1234.5678");
    }

    // ── Utf8 / strings ────────────────────────────────────────────────────────

    #[test]
    fn utf8_plain_string_wrapped_in_n_quotes() {
        let arr: Arc<dyn arrow::array::Array> = Arc::new(StringArray::from(vec!["hello"]));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "N'hello'");
    }

    #[test]
    fn utf8_single_quote_escaped() {
        let arr: Arc<dyn arrow::array::Array> = Arc::new(StringArray::from(vec!["it's"]));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "N'it''s'");
    }

    #[test]
    fn utf8_empty_string() {
        let arr: Arc<dyn arrow::array::Array> = Arc::new(StringArray::from(vec![""]));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "N''");
    }

    // ── Binary ────────────────────────────────────────────────────────────────

    #[test]
    fn binary_bytes_to_hex_literal() {
        let mut b = BinaryBuilder::new();
        b.append_value(&[0xDE_u8, 0xAD, 0xBE, 0xEF]);
        let arr: Arc<dyn arrow::array::Array> = Arc::new(b.finish());
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "0xDEADBEEF");
    }

    #[test]
    fn binary_empty_bytes_to_0x() {
        let mut b = BinaryBuilder::new();
        b.append_value(&[] as &[u8]);
        let arr: Arc<dyn arrow::array::Array> = Arc::new(b.finish());
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "0x");
    }

    // ── Timestamp ─────────────────────────────────────────────────────────────

    #[test]
    fn timestamp_epoch_zero() {
        let mut b = TimestampMicrosecondBuilder::new();
        b.append_value(0); // epoch 0 microseconds
        let arr: Arc<dyn arrow::array::Array> = Arc::new(
            b.finish()
                .with_timezone("UTC".to_string()),
        );
        let result = arrow_value_to_sql(arr.as_ref(), 0);
        assert_eq!(result, "'1970-01-01T00:00:00.000000Z'");
    }

    #[test]
    fn timestamp_known_value() {
        // 1_000_000 microseconds = 1 second after epoch
        let mut b = TimestampMicrosecondBuilder::new();
        b.append_value(1_000_000);
        let arr: Arc<dyn arrow::array::Array> = Arc::new(
            b.finish()
                .with_timezone("UTC".to_string()),
        );
        let result = arrow_value_to_sql(arr.as_ref(), 0);
        assert_eq!(result, "'1970-01-01T00:00:01.000000Z'");
    }

    // ── Date32 ────────────────────────────────────────────────────────────────

    #[test]
    fn date32_day_0_is_epoch() {
        let arr = scalar(Date32Builder::new(), |b| b.append_value(0));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "'1970-01-01'");
    }

    #[test]
    fn date32_day_1_is_jan_2() {
        let arr = scalar(Date32Builder::new(), |b| b.append_value(1));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "'1970-01-02'");
    }

    // ── Time64 ────────────────────────────────────────────────────────────────

    #[test]
    fn time64_zero_micros_is_midnight() {
        let arr = scalar(Time64MicrosecondBuilder::new(), |b| b.append_value(0));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "'00:00:00.000000'");
    }

    #[test]
    fn time64_3661000001_micros() {
        // 3661000001 us = 1h + 1m + 1s + 1us → '01:01:01.000001'
        let arr = scalar(Time64MicrosecondBuilder::new(), |b| b.append_value(3_661_000_001));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "'01:01:01.000001'");
    }

    // ── validate_fqn ─────────────────────────────────────────────────────────

    #[test]
    fn valid_fqn_simple() {
        assert!(validate_fqn("[dbo].[Orders]").is_ok());
    }

    #[test]
    fn valid_fqn_with_spaces() {
        assert!(validate_fqn("[my schema].[My Table]").is_ok());
    }

    #[test]
    fn invalid_fqn_no_brackets() {
        assert!(validate_fqn("dbo.Orders").is_err());
    }

    #[test]
    fn invalid_fqn_sql_injection() {
        assert!(validate_fqn("[dbo].[t]; DROP TABLE users--").is_err());
    }

    #[test]
    fn invalid_fqn_empty_string() {
        assert!(validate_fqn("").is_err());
    }

    #[test]
    fn invalid_fqn_missing_schema() {
        assert!(validate_fqn("[Orders]").is_err());
    }

    // ── validate_database_name ────────────────────────────────────────────────

    #[test]
    fn valid_db_name_alpha() {
        assert!(validate_database_name("mydb").is_ok());
    }

    #[test]
    fn valid_db_name_with_underscore_and_digits() {
        assert!(validate_database_name("my_db_2").is_ok());
    }

    #[test]
    fn invalid_db_name_hyphen() {
        assert!(validate_database_name("my-db").is_err());
    }

    #[test]
    fn invalid_db_name_sql_injection() {
        assert!(validate_database_name("db; DROP DATABASE master--").is_err());
    }

    #[test]
    fn invalid_db_name_empty() {
        assert!(validate_database_name("").is_err());
    }

    // ── split_schema_sql ──────────────────────────────────────────────────────

    #[test]
    fn split_with_marker_separates_correctly() {
        let marker = crate::schema::POST_DATA_MARKER;
        let sql = format!("CREATE TABLE Foo ();\n{marker}\nALTER TABLE Foo ADD CONSTRAINT fk;");
        let (pre, post) = split_schema_sql(&sql);
        assert!(pre.contains("CREATE TABLE Foo"), "pre={pre:?}");
        assert!(!pre.contains(marker), "marker must not be in pre");
        assert!(post.contains("ALTER TABLE"), "post={post:?}");
        assert!(!post.contains("CREATE TABLE"), "pre DDL must not leak into post");
    }

    #[test]
    fn split_without_marker_returns_whole_sql_in_pre() {
        let sql = "CREATE TABLE Foo ();\nCREATE TABLE Bar ();";
        let (pre, post) = split_schema_sql(sql);
        assert_eq!(pre, sql);
        assert_eq!(post, "");
    }

    #[test]
    fn split_empty_string() {
        let (pre, post) = split_schema_sql("");
        assert_eq!(pre, "");
        assert_eq!(post, "");
    }
}
