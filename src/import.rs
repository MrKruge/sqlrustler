use anyhow::{bail, Context, Result};
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

    if !(1..=2).contains(&manifest.format_version) {
        bail!(
            "Unsupported archive format_version {} (this build reads 1-2)",
            manifest.format_version
        );
    }

    // 2. Split schema at POST_DATA marker
    let (pre_data_ddl, post_data_ddl) = split_schema_sql(&schema_sql);

    // 2b. Validate everything that will be interpolated into / executed as SQL BEFORE any
    // destructive step (--drop-existing). The archive is untrusted input.
    for entry in &manifest.tables {
        validate_fqn(&entry.table_fqn)?;
    }
    if !args.data_only {
        // schema.sql is only executed when !data_only, so only validate it then.
        validate_ddl_script(pre_data_ddl)
            .context("Refusing to execute schema.sql (pre-data section)")?;
        validate_ddl_script(post_data_ddl)
            .context("Refusing to execute schema.sql (post-data section)")?;
    }

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
        execute_batched_sql(&pool, pre_data_ddl)
            .await
            .context("Failed to execute schema DDL")?;
    }

    // 6. Import table data in parallel
    let semaphore = Arc::new(Semaphore::new(args.parallel));
    let mut handles = Vec::new();
    let mut total_rows_imported = 0u64;

    for entry in &manifest.tables {
        if entry.row_count == 0 {
            continue;
        }

        // Collect all part files (v1 archives have one, v2 chunked archives may have many).
        // Each part is a complete, separate Parquet file and must be read on its own.
        // A missing part is a hard error: skipping it would silently import partial data.
        let parts = read_all_parts(&entry.table_fqn, &entry.all_file_names(), |name| {
            archive.read_parquet(name)
        })?;
        let expected_rows = entry.row_count;

        let permit = semaphore.clone().acquire_owned().await?;
        let pool = pool.clone();
        let table_fqn = entry.table_fqn.clone();
        let batch_size = args.batch_size;
        let has_identity = entry.has_identity;

        let handle = tokio::spawn(async move {
            let _permit = permit;
            let rows =
                import_table(pool, table_fqn.clone(), parts, batch_size, has_identity).await?;
            check_imported_rows(&table_fqn, expected_rows, rows)?;
            Ok::<u64, anyhow::Error>(rows)
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
/// Accepts only `[schema].[table]` where each part is non-empty and contains no `]`
/// (so it can never terminate the bracket-quoted identifier), no `[`, and no control
/// characters. Hyphens, dots, `$`, `#` etc. are legal in SQL Server names and allowed.
/// Rejects anything that could be used to inject SQL.
fn validate_fqn(fqn: &str) -> Result<()> {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r"^\[[^\[\]\x00-\x1f\x7f]+\]\.\[[^\[\]\x00-\x1f\x7f]+\]$").unwrap()
    });
    if re.is_match(fqn) {
        Ok(())
    } else {
        bail!("Untrusted table FQN in archive manifest: {fqn:?} — import aborted")
    }
}

/// Bracket-quote an identifier, doubling any `]` so it cannot break out of the quoting.
/// Used for column names, which come from the (untrusted) Parquet schema.
fn quote_ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// Validate a database name before interpolating into SQL (used with --drop-existing).
/// Accepts only alphanumeric characters and underscores.
fn validate_database_name(name: &str) -> Result<()> {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"^[\w]+$").unwrap());
    if !re.is_match(name) || name.len() > 128 {
        bail!("Invalid database name: {name:?} — only word characters (a-z, 0-9, _) are allowed");
    }
    // --drop-existing must never be able to target a system database.
    if ["master", "tempdb", "model", "msdb"].contains(&name.to_ascii_lowercase().as_str()) {
        bail!("Refusing to drop/recreate system database {name:?}");
    }
    Ok(())
}

/// Read every part file of a table via `read`. Errors if the table lists no parts or if any
/// part cannot be read, so an import can never succeed with part of a table missing.
fn read_all_parts(
    table_fqn: &str,
    names: &[&str],
    read: impl Fn(&str) -> Result<Vec<u8>>,
) -> Result<Vec<Vec<u8>>> {
    if names.is_empty() {
        bail!("{table_fqn}: manifest lists rows but no data files — archive is incomplete");
    }
    names
        .iter()
        .map(|n| read(n).with_context(|| format!("{table_fqn}: cannot read part {n}")))
        .collect()
}

/// The rows actually inserted must equal the manifest row count for the table.
fn check_imported_rows(table_fqn: &str, expected: u64, actual: u64) -> Result<()> {
    if expected != actual {
        bail!("{table_fqn}: imported {actual} rows but the manifest records {expected} — archive and import disagree");
    }
    Ok(())
}

async fn import_table(
    pool: crate::connection::DbPool,
    table_fqn: String,
    parquet_parts: Vec<Vec<u8>>,
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
        conn.execute(&format!("SET IDENTITY_INSERT {table_fqn} ON"), &[])
            .await
            .with_context(|| format!("Failed to enable IDENTITY_INSERT for {table_fqn}"))?;
    }

    // Run the actual data load. If it fails, we still need to turn IDENTITY_INSERT off
    // so the connection is usable when returned to the pool.
    let mut result: Result<u64> = Ok(0);
    for part in parquet_parts {
        match import_table_data(&mut conn, &table_fqn, part, batch_size).await {
            Ok(n) => result = result.map(|t| t + n),
            Err(e) => {
                result = Err(e);
                break;
            }
        }
    }

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
        .map(|f| quote_ident(f.name()))
        .collect();

    let mut row_values: Vec<String> = Vec::with_capacity(batch.num_rows());

    for row_idx in 0..batch.num_rows() {
        let mut col_vals: Vec<String> = Vec::with_capacity(batch.num_columns());

        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx);
            let val = try_arrow_value_to_sql(col, row_idx).with_context(|| {
                format!("column {:?}, row {row_idx}", schema.field(col_idx).name())
            })?;
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

/// Downcast helper: a malformed Parquet file must produce an error, never a panic.
fn downcast<T: 'static>(col: &dyn arrow::array::Array) -> Result<&T> {
    col.as_any().downcast_ref::<T>().ok_or_else(|| {
        anyhow::anyhow!(
            "Parquet column has unexpected array type for {:?}",
            col.data_type()
        )
    })
}

/// Convert a single cell from an Arrow array to a SQL literal string.
/// Returns "NULL" for nulls. Errors (instead of panicking or silently writing NULL/1970)
/// on any value that cannot be represented faithfully — the Parquet file is untrusted.
/// Every branch emits either a number, a hex literal, a fixed-format date/time, or a
/// `N'...'` literal with `'` doubled, so no cell value can terminate the literal.
fn try_arrow_value_to_sql(col: &dyn arrow::array::Array, row_idx: usize) -> Result<String> {
    use arrow::datatypes::TimeUnit;
    if col.is_null(row_idx) {
        return Ok("NULL".to_string());
    }

    Ok(match col.data_type() {
        DataType::Boolean => {
            if downcast::<BooleanArray>(col)?.value(row_idx) {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        DataType::Int8 => downcast::<Int8Array>(col)?.value(row_idx).to_string(),
        DataType::UInt8 => downcast::<UInt8Array>(col)?.value(row_idx).to_string(),
        DataType::Int16 => downcast::<Int16Array>(col)?.value(row_idx).to_string(),
        DataType::Int32 => downcast::<Int32Array>(col)?.value(row_idx).to_string(),
        DataType::Int64 => downcast::<Int64Array>(col)?.value(row_idx).to_string(),
        DataType::Float32 => {
            let v = downcast::<Float32Array>(col)?.value(row_idx);
            if !v.is_finite() {
                bail!("Non-finite float ({v}) cannot be stored in SQL Server");
            }
            v.to_string()
        }
        DataType::Float64 => {
            let v = downcast::<Float64Array>(col)?.value(row_idx);
            if !v.is_finite() {
                bail!("Non-finite float ({v}) cannot be stored in SQL Server");
            }
            v.to_string()
        }
        DataType::Decimal128(_, scale) => {
            let raw = downcast::<Decimal128Array>(col)?.value(row_idx);
            // Scale comes from the file: bound it so 10^scale cannot overflow and the
            // zero-padding width cannot request a gigantic allocation.
            if !(0..=38).contains(scale) {
                bail!("Decimal128 scale {scale} out of range 0..=38");
            }
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
                format!(
                    "{sign}{}.{:0>width$}",
                    int_part,
                    frac_part,
                    width = s as usize
                )
            }
        }
        DataType::Utf8 => {
            let s = downcast::<StringArray>(col)?
                .value(row_idx)
                .replace('\'', "''"); // escape single quotes
            format!("N'{s}'")
        }
        DataType::Binary => {
            let hex: String = downcast::<BinaryArray>(col)?
                .value(row_idx)
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect();
            format!("0x{hex}")
        }
        DataType::FixedSizeBinary(_) => {
            let hex: String = downcast::<FixedSizeBinaryArray>(col)?
                .value(row_idx)
                .iter()
                .map(|b| format!("{:02X}", b))
                .collect();
            format!("0x{hex}")
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = downcast::<TimestampMicrosecondArray>(col)?.value(row_idx);
            // Euclidean split so pre-1970 values keep a non-negative fraction
            // (-1.5s => secs=-2, frac=0.5s), instead of truncating toward zero.
            let secs = micros.div_euclid(1_000_000);
            let frac = micros.rem_euclid(1_000_000);
            // Format as ISO 8601 with explicit UTC suffix so SQL Server knows the timezone.
            // All timestamps are stored as UTC in Parquet (datetimeoffset converted to UTC on export).
            let dt =
                chrono::DateTime::from_timestamp(secs, (frac * 1000) as u32).ok_or_else(|| {
                    anyhow::anyhow!("Timestamp {micros}us out of representable range")
                })?;
            format!("'{}'", dt.format("%Y-%m-%dT%H:%M:%S%.6fZ"))
        }
        DataType::Date32 => {
            let days = downcast::<Date32Array>(col)?.value(row_idx);
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = chrono::Duration::try_days(days as i64)
                .and_then(|d| epoch.checked_add_signed(d))
                .ok_or_else(|| anyhow::anyhow!("Date32 value {days} out of representable range"))?;
            format!("'{}'", date.format("%Y-%m-%d"))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let micros = downcast::<Time64MicrosecondArray>(col)?.value(row_idx);
            if !(0..86_400_000_000).contains(&micros) {
                bail!("Time64 value {micros}us outside 00:00:00..24:00:00");
            }
            let h = micros / 3_600_000_000;
            let m = (micros % 3_600_000_000) / 60_000_000;
            let s = (micros % 60_000_000) / 1_000_000;
            let us = micros % 1_000_000;
            format!("'{h:02}:{m:02}:{s:02}.{us:06}'")
        }
        other => bail!("Unsupported Arrow type in Parquet file: {other:?}"),
    })
}

/// Test convenience wrapper (panics on error).
#[cfg(test)]
fn arrow_value_to_sql(col: &dyn arrow::array::Array, row_idx: usize) -> String {
    try_arrow_value_to_sql(col, row_idx).unwrap()
}

// ── schema.sql validation ─────────────────────────────────────────────────────
// schema.sql comes from the archive and is executed verbatim, so a tampered archive
// would otherwise be arbitrary SQL execution as the importing login. We check each GO
// batch structurally: batches that define a view/procedure/function are accepted (their
// body is only *defined*, not run); every other batch may only contain table / index /
// constraint DDL. Bodies of procedures remain unrestricted — archives must still come
// from a trusted source. Opt-out: SQLRUSTLER_SKIP_DDL_CHECK=1.

const GO_BATCH_PATTERN: &str = r"(?mi)^\s*GO\s*$";

/// Tokenise T-SQL into top-level words, skipping comments, '...' strings,
/// [...] / "..." quoted identifiers and numbers. Errors on unterminated constructs.
fn sql_words(sql: &str) -> Result<Vec<String>> {
    let b: Vec<char> = sql.chars().collect();
    let mut i = 0;
    let mut words = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c == '-' && b.get(i + 1) == Some(&'-') {
            while i < b.len() && b[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && b.get(i + 1) == Some(&'*') {
            let mut depth = 1;
            i += 2;
            while depth > 0 {
                if i + 1 >= b.len() {
                    bail!("unterminated /* comment in schema.sql");
                }
                if b[i] == '/' && b[i + 1] == '*' {
                    depth += 1;
                    i += 2;
                } else if b[i] == '*' && b[i + 1] == '/' {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        } else if c == '\'' || c == '[' || c == '"' {
            let close = if c == '[' { ']' } else { c };
            i += 1;
            loop {
                if i >= b.len() {
                    bail!("unterminated quoted literal/identifier in schema.sql");
                }
                if b[i] == close {
                    if b.get(i + 1) == Some(&close) {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
        } else if c.is_alphabetic() || c == '_' || c == '@' || c == '#' {
            let st = i;
            while i < b.len() && (b[i].is_alphanumeric() || matches!(b[i], '_' | '@' | '#' | '$')) {
                i += 1;
            }
            words.push(b[st..i].iter().collect::<String>().to_ascii_uppercase());
        } else if c.is_ascii_digit() {
            while i < b.len() && (b[i].is_alphanumeric() || b[i] == '.') {
                i += 1;
            }
        } else {
            i += 1;
        }
    }
    Ok(words)
}

const DDL_DENY_WORDS: &[&str] = &[
    "EXEC",
    "EXECUTE",
    "INSERT",
    "MERGE",
    "GRANT",
    "REVOKE",
    "DENY",
    "TRUNCATE",
    "SHUTDOWN",
    "BACKUP",
    "RESTORE",
    "DBCC",
    "OPENROWSET",
    "OPENQUERY",
    "OPENDATASOURCE",
    "BULK",
    "KILL",
    "WAITFOR",
    "USE",
    "DECLARE",
    "SELECT",
    "FROM",
    "RECONFIGURE",
    "LOGIN",
    "USER",
    "DATABASE",
    "SERVER",
    "CREDENTIAL",
    "ROLE",
    "MEMBER",
    "IMPERSONATE",
    "XP_CMDSHELL",
];

pub(crate) fn validate_ddl_script(sql: &str) -> Result<()> {
    if std::env::var("SQLRUSTLER_SKIP_DDL_CHECK")
        .map(|v| v == "1")
        .unwrap_or(false)
    {
        tracing::warn!("SQLRUSTLER_SKIP_DDL_CHECK=1: schema.sql validation DISABLED");
        return Ok(());
    }
    let go = Regex::new(GO_BATCH_PATTERN).unwrap();
    for batch in go.split(sql) {
        let w = sql_words(batch)?;
        let Some(first) = w.first() else { continue };
        let at = |i: usize| w.get(i).map(String::as_str).unwrap_or("");

        // Definition batches (CREATE|ALTER [OR ALTER] VIEW/PROC/FUNCTION): body is not executed.
        if first == "CREATE" || first == "ALTER" {
            let mut k = 1;
            if at(k) == "OR" && at(k + 1) == "ALTER" {
                k += 2;
            }
            if matches!(at(k), "VIEW" | "PROC" | "PROCEDURE" | "FUNCTION") {
                continue;
            }
        }

        for (i, word) in w.iter().enumerate() {
            let (prev, next) = (if i > 0 { at(i - 1) } else { "" }, at(i + 1));
            let ok = match word.as_str() {
                "CREATE" => matches!(
                    next,
                    "TABLE"
                        | "INDEX"
                        | "UNIQUE"
                        | "CLUSTERED"
                        | "NONCLUSTERED"
                        | "SCHEMA"
                        | "TYPE"
                        | "SEQUENCE"
                ),
                "ALTER" => next == "TABLE",
                "DROP" => matches!(next, "CONSTRAINT" | "INDEX"),
                "UPDATE" | "DELETE" => prev == "ON",
                "SET" => matches!(prev, "UPDATE" | "DELETE"),
                w if DDL_DENY_WORDS.contains(&w) => false,
                w if w.starts_with("XP_") || w.starts_with("SP_") => false,
                _ => true,
            };
            if !ok {
                bail!("schema.sql contains a statement not permitted in an import archive near {:?} {:?}", word, next);
            }
        }
    }
    Ok(())
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
    let batch_re = regex::Regex::new(GO_BATCH_PATTERN).unwrap();

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

    fn scalar<B: arrow::array::ArrayBuilder>(
        mut builder: B,
        append: impl Fn(&mut B),
    ) -> Arc<dyn arrow::array::Array> {
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
        let mut builder = Decimal128Builder::new()
            .with_precision_and_scale(18, 4)
            .unwrap();
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
        b.append_value([0xDE_u8, 0xAD, 0xBE, 0xEF]);
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
        let arr: Arc<dyn arrow::array::Array> =
            Arc::new(b.finish().with_timezone("UTC".to_string()));
        let result = arrow_value_to_sql(arr.as_ref(), 0);
        assert_eq!(result, "'1970-01-01T00:00:00.000000Z'");
    }

    #[test]
    fn timestamp_known_value() {
        // 1_000_000 microseconds = 1 second after epoch
        let mut b = TimestampMicrosecondBuilder::new();
        b.append_value(1_000_000);
        let arr: Arc<dyn arrow::array::Array> =
            Arc::new(b.finish().with_timezone("UTC".to_string()));
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
        let arr = scalar(Time64MicrosecondBuilder::new(), |b| {
            b.append_value(3_661_000_001)
        });
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

    // ── read_all_parts / check_imported_rows ─────────────────────────────────

    #[test]
    fn read_all_parts_returns_each_part_separately_in_order() {
        let parts =
            read_all_parts("[dbo].[T]", &["a", "b"], |n| Ok(n.as_bytes().to_vec())).unwrap();
        assert_eq!(parts, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn read_all_parts_errors_when_a_part_is_missing() {
        let r = read_all_parts("[dbo].[T]", &["a", "b"], |n| {
            if n == "b" {
                anyhow::bail!("not found")
            } else {
                Ok(vec![1])
            }
        });
        assert!(r.is_err());
    }

    #[test]
    fn read_all_parts_errors_when_no_parts_listed() {
        assert!(read_all_parts("[dbo].[T]", &[], |_| Ok(vec![])).is_err());
    }

    #[test]
    fn imported_row_count_must_match_manifest() {
        assert!(check_imported_rows("[dbo].[T]", 10, 10).is_ok());
        assert!(check_imported_rows("[dbo].[T]", 10, 9).is_err());
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
        assert!(
            !post.contains("CREATE TABLE"),
            "pre DDL must not leak into post"
        );
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

    // ── Security hardening (audit round 1) ───────────────────────────────────

    #[test]
    fn fqn_allows_hyphen_dot_and_dollar_names() {
        assert!(validate_fqn("[my-schema].[Order.Items$1]").is_ok());
    }

    #[test]
    fn fqn_rejects_bracket_breakout_and_extra_parts() {
        for bad in [
            "[dbo].[a]]; DROP TABLE x;--]",
            "[dbo].[a].[b]",
            "[d]bo].[t]",
            "[dbo].[t] ",
            "[dbo].[t]\n",
            "[dbo].[]",
            "[].[t]",
            "[dbo].[t\0]",
            "[dbo].[a[b]",
        ] {
            assert!(validate_fqn(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn quote_ident_doubles_closing_bracket() {
        assert_eq!(quote_ident("id"), "[id]");
        assert_eq!(quote_ident("a]b"), "[a]]b]");
        assert_eq!(quote_ident("x]; DROP TABLE t;--"), "[x]]; DROP TABLE t;--]");
    }

    #[test]
    fn db_name_rejects_system_databases_and_overlong() {
        for n in ["master", "MASTER", "tempdb", "model", "msdb"] {
            assert!(validate_database_name(n).is_err(), "{n}");
        }
        assert!(validate_database_name(&"a".repeat(129)).is_err());
        assert!(validate_database_name(&"a".repeat(128)).is_ok());
        assert!(validate_database_name("master2").is_ok());
    }

    #[test]
    fn utf8_injection_payload_stays_inside_literal() {
        let arr: Arc<dyn arrow::array::Array> =
            Arc::new(StringArray::from(vec!["x'); DROP TABLE t;--"]));
        assert_eq!(
            arrow_value_to_sql(arr.as_ref(), 0),
            "N'x''); DROP TABLE t;--'"
        );
    }

    #[test]
    fn utf8_backslash_and_unicode_not_altered() {
        let arr: Arc<dyn arrow::array::Array> =
            Arc::new(StringArray::from(vec!["a\\'b\u{2019}\u{0}"]));
        assert_eq!(
            arrow_value_to_sql(arr.as_ref(), 0),
            "N'a\\''b\u{2019}\u{0}'"
        );
    }

    #[test]
    fn unsupported_type_errors_instead_of_silent_null() {
        let arr: Arc<dyn arrow::array::Array> =
            Arc::new(arrow::array::LargeStringArray::from(vec!["x"]));
        assert!(try_arrow_value_to_sql(arr.as_ref(), 0).is_err());
    }

    #[test]
    fn non_finite_floats_error() {
        let arr: Arc<dyn arrow::array::Array> = Arc::new(Float64Array::from(vec![f64::NAN]));
        assert!(try_arrow_value_to_sql(arr.as_ref(), 0).is_err());
        let arr: Arc<dyn arrow::array::Array> = Arc::new(Float32Array::from(vec![f32::INFINITY]));
        assert!(try_arrow_value_to_sql(arr.as_ref(), 0).is_err());
        let arr: Arc<dyn arrow::array::Array> = Arc::new(Float64Array::from(vec![1.5]));
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "1.5");
    }

    #[test]
    fn decimal_scale_out_of_range_errors_not_panics() {
        // Arrow rejects scale>precision at build time; a hostile file can still carry odd
        // scales, so exercise the bounds via negative scale (valid in Arrow).
        let mut b = Decimal128Builder::new()
            .with_precision_and_scale(10, -2)
            .unwrap();
        b.append_value(5);
        let arr: Arc<dyn arrow::array::Array> = Arc::new(b.finish());
        assert!(try_arrow_value_to_sql(arr.as_ref(), 0).is_err());
    }

    #[test]
    fn timestamp_pre_epoch_fraction_is_correct() {
        // -1.5s => 1969-12-31T23:59:58.500000Z
        let mut b = TimestampMicrosecondBuilder::new();
        b.append_value(-1_500_000);
        let arr: Arc<dyn arrow::array::Array> =
            Arc::new(b.finish().with_timezone("UTC".to_string()));
        assert_eq!(
            arrow_value_to_sql(arr.as_ref(), 0),
            "'1969-12-31T23:59:58.500000Z'"
        );
    }

    #[test]
    fn timestamp_out_of_range_errors() {
        let mut b = TimestampMicrosecondBuilder::new();
        b.append_value(i64::MAX);
        let arr: Arc<dyn arrow::array::Array> =
            Arc::new(b.finish().with_timezone("UTC".to_string()));
        assert!(try_arrow_value_to_sql(arr.as_ref(), 0).is_err());
    }

    #[test]
    fn date32_extreme_errors_not_panics() {
        let arr = scalar(Date32Builder::new(), |b| b.append_value(i32::MAX));
        assert!(try_arrow_value_to_sql(arr.as_ref(), 0).is_err());
        let arr = scalar(Date32Builder::new(), |b| b.append_value(i32::MIN));
        assert!(try_arrow_value_to_sql(arr.as_ref(), 0).is_err());
    }

    #[test]
    fn time64_out_of_range_errors() {
        let arr = scalar(Time64MicrosecondBuilder::new(), |b| b.append_value(-1));
        assert!(try_arrow_value_to_sql(arr.as_ref(), 0).is_err());
        let arr = scalar(Time64MicrosecondBuilder::new(), |b| {
            b.append_value(86_400_000_000)
        });
        assert!(try_arrow_value_to_sql(arr.as_ref(), 0).is_err());
    }

    #[test]
    fn non_microsecond_timestamp_errors_not_panics() {
        let arr: Arc<dyn arrow::array::Array> =
            Arc::new(arrow::array::TimestampNanosecondArray::from(vec![1i64]));
        assert!(try_arrow_value_to_sql(arr.as_ref(), 0).is_err());
    }

    #[test]
    fn fixed_size_binary_to_hex() {
        let arr: Arc<dyn arrow::array::Array> = Arc::new(
            FixedSizeBinaryArray::try_from_iter(vec![vec![0x01u8, 0xAB]].into_iter()).unwrap(),
        );
        assert_eq!(arrow_value_to_sql(arr.as_ref(), 0), "0x01AB");
    }

    // ── validate_ddl_script ───────────────────────────────────────────────────

    const GENERATED_SCHEMA: &str = "-- sqlrustler schema export\n-- Generated: 2026-01-01\n\n\
-- === DROP FOREIGN KEYS (reverse topo order) ===\n\
ALTER TABLE [dbo].[Child] DROP CONSTRAINT IF EXISTS [fk_Child_Parent];\n\n\
-- === CREATE TABLES (topo order) ===\n\
CREATE TABLE [dbo].[Parent] (\n    [Id] int IDENTITY(1,1) NOT NULL,\n    [Name] nvarchar(100) NULL,\n    [Price] decimal(18,4) NOT NULL\n);\nGO\n\
CREATE TABLE [dbo].[Child] (\n    [Id] int NOT NULL,\n    [ParentId] int NOT NULL\n);\nGO\n\n\
-- === VIEWS / STORED PROCS / FUNCTIONS ===\n\
CREATE VIEW dbo.v AS SELECT Id FROM dbo.Parent\nGO\n\
CREATE PROCEDURE dbo.p AS BEGIN SELECT 1; EXEC sp_who; END\nGO\n\
-- SQLRUSTLER_SECTION: POST_DATA\n\n\
ALTER TABLE [dbo].[Child] ADD CONSTRAINT [fk_Child_Parent] FOREIGN KEY ([ParentId]) REFERENCES [dbo].[Parent] ([Id]) ON DELETE CASCADE ON UPDATE SET NULL;";

    #[test]
    fn ddl_check_accepts_real_generator_fk_drop_and_schema_guard() {
        let fk = crate::schema::build_fk(
            "fk_c_p",
            ("sales", "Child"),
            ("sales", "Parent"),
            &["[a]".into()],
            &["[x]".into()],
            "NO_ACTION",
            "NO_ACTION",
            false,
            false,
        );
        let sql = format!("{};\nGO\n{};", fk.drop_stmt, fk.alter_stmt);
        validate_ddl_script(&sql).unwrap();
    }

    #[test]
    fn ddl_check_accepts_generator_output() {
        let (pre, post) = split_schema_sql(GENERATED_SCHEMA);
        validate_ddl_script(pre).unwrap();
        validate_ddl_script(post).unwrap();
    }

    #[test]
    fn ddl_check_rejects_tampered_statements() {
        for bad in [
            "CREATE TABLE [dbo].[t] (a int);\nDROP DATABASE prod;",
            "CREATE TABLE [dbo].[t] (a int);\nGO\nDROP TABLE [dbo].[users];",
            "CREATE TABLE [dbo].[t] (a int);\nGO\nEXEC xp_cmdshell 'whoami';",
            "GO\nEXEC sp_addsrvrolemember 'x','sysadmin'",
            "CREATE LOGIN evil WITH PASSWORD='x'",
            "CREATE USER evil",
            "UPDATE [dbo].[users] SET admin = 1",
            "DELETE FROM [dbo].[users]",
            "INSERT INTO [dbo].[users] VALUES (1)",
            "SELECT * FROM OPENROWSET('x','y','z')",
            "TRUNCATE TABLE [dbo].[users]",
            "GRANT CONTROL TO evil",
            "ALTER ROLE db_owner ADD MEMBER evil",
            "ALTER DATABASE x SET SINGLE_USER",
            "ALTER TABLE [dbo].[t] ADD c int; EXEC('DROP TABLE x')",
            "CREATE TABLE [dbo].[t] (a int) /* unterminated",
            "CREATE TABLE [dbo].[t] (a int); SELECT 'unterminated",
        ] {
            assert!(validate_ddl_script(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn ddl_check_ignores_keywords_in_strings_comments_and_identifiers() {
        let sql = "-- DROP DATABASE x\n/* EXEC xp_cmdshell */\n\
                   CREATE TABLE [dbo].[select] ([exec] int, [drop]] database] int NOT NULL);";
        validate_ddl_script(sql).unwrap();
    }

    #[test]
    fn ddl_check_go_split_matches_executor() {
        // A GO inside the script must split exactly as execute_batched_sql splits it, so a
        // dangerous batch can't hide behind a batch that looks like a view definition.
        let sql = "CREATE VIEW v AS SELECT 1\nGO\nDROP TABLE [dbo].[t]";
        assert!(validate_ddl_script(sql).is_err());
    }

    #[test]
    fn ddl_check_empty_ok() {
        validate_ddl_script("").unwrap();
        validate_ddl_script("-- only a comment\n").unwrap();
    }

    // ── Round 2: re-audit of the DDL gate ─────────────────────────────────────

    #[test]
    fn ddl_check_is_case_insensitive_and_catches_same_line_chaining() {
        for bad in [
            "create table [dbo].[t] (a int) drop database prod",
            "cReAtE tAbLe [dbo].[t] (a int); eXeC('x')",
            "ALTER TABLE [dbo].[t] ADD CONSTRAINT c CHECK (a IN (SELECT 1))",
            "CREATE TABLE [dbo].[t] (a int);\n  go  \nDROP TABLE [dbo].[t]",
            "CREATE TABLE [dbo].[t] (a int); WAITFOR DELAY '00:10'",
        ] {
            assert!(validate_ddl_script(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn ddl_check_allows_fk_referential_actions_and_indexes() {
        validate_ddl_script(
            "ALTER TABLE [dbo].[c] ADD CONSTRAINT [fk] FOREIGN KEY ([a]) REFERENCES [dbo].[p] ([id]) \
             ON DELETE SET NULL ON UPDATE SET DEFAULT;\n\
             CREATE UNIQUE NONCLUSTERED INDEX [ix] ON [dbo].[c] ([a]);",
        )
        .unwrap();
    }

    #[test]
    fn ddl_check_definition_batches_may_contain_any_body() {
        validate_ddl_script(
            "CREATE OR ALTER PROCEDURE dbo.p AS BEGIN DELETE FROM dbo.t; EXEC sp_who; END",
        )
        .unwrap();
        // ...but a definition keyword later in a batch does not exempt the batch.
        assert!(validate_ddl_script("DROP TABLE dbo.t; CREATE VIEW v AS SELECT 1").is_err());
    }
}
