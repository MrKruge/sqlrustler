//! `sqlrustler test` — quick three-layer diagnostic.
//!
//! Answers: which of these is holding you back?
//!
//!   destination  — local machine: can it encode and compress Parquet fast enough?
//!   bandwidth    — network pipe: how many MB/s flows from Azure SQL to here?
//!   source       — Azure SQL: connection latency, query throughput, table counts
//!
//! Each test runs in under 30 seconds and prints a clear pass/warn/fail verdict.

use anyhow::{Context, Result};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Instant;

use crate::cli::TestArgs;
use crate::connection::build_pool;
use crate::schema;
use crate::types::select_expr;

// ── verdict helpers ───────────────────────────────────────────────────────────

fn section(title: &str) {
    println!("\n┌─ {} {}", title, "─".repeat(55usize.saturating_sub(title.len() + 3)));
}

fn ok(label: &str, value: &str) {
    println!("│  ✓  {:<32} {}", label, value);
}

fn warn(label: &str, value: &str, advice: &str) {
    println!("│  ⚠  {:<32} {}  ← {}", label, value, advice);
}

fn verdict(pass: bool, msg: &str) {
    if pass {
        println!("└─ PASS  {msg}");
    } else {
        println!("└─ WARN  {msg}");
    }
}

// ── destination test ──────────────────────────────────────────────────────────

fn test_destination(rows: usize) -> Result<()> {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    section("DESTINATION  (local Parquet encode + zstd write speed)");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id",    DataType::Int64, false),
        Field::new("val_a", DataType::Int64, true),
        Field::new("val_b", DataType::Utf8,  true),
        Field::new("val_c", DataType::Utf8,  true),
        Field::new("val_d", DataType::Int64, true),
    ]));

    let tmp = tempfile::NamedTempFile::new()?;
    let file = std::fs::File::create(tmp.path())?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3)?))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let batch_size = 100_000usize;
    let mut written = 0usize;
    let t0 = Instant::now();

    while written < rows {
        let n = batch_size.min(rows - written);
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new((0..n as i64).collect::<Int64Array>()),
            Arc::new((0..n as i64).map(|i| Some(i * 7)).collect::<Int64Array>()),
            Arc::new((0..n).map(|i| Some(format!("str_{i}"))).collect::<StringArray>()),
            Arc::new((0..n).map(|i| Some(format!("val_{i}"))).collect::<StringArray>()),
            Arc::new((0..n as i64).map(|i| Some(i % 1000)).collect::<Int64Array>()),
        ])?;
        writer.write(&batch)?;
        written += n;
    }
    writer.close()?;

    let elapsed = t0.elapsed();
    let file_bytes = std::fs::metadata(tmp.path())?.len();
    let rows_per_sec = written as f64 / elapsed.as_secs_f64().max(0.001);
    let mb_per_sec   = file_bytes as f64 / elapsed.as_secs_f64().max(0.001) / 1_048_576.0;

    ok("rows written",  &format!("{written:>12}"));
    ok("elapsed",       &format!("{:.2}s", elapsed.as_secs_f64()));
    ok("rows/sec",      &format!("{rows_per_sec:>12.0}"));
    ok("output size",   &bytesize::ByteSize(file_bytes).to_string());

    let pass = rows_per_sec >= 500_000.0;
    if pass {
        ok("write speed", &format!("{mb_per_sec:.1} MB/s  — local machine is not the bottleneck"));
    } else {
        warn("write speed", &format!("{mb_per_sec:.1} MB/s"), "local disk or CPU may limit export speed");
    }

    verdict(pass, &format!("{rows_per_sec:.0} rows/s local Parquet write"));
    Ok(())
}

// ── bandwidth test ────────────────────────────────────────────────────────────

async fn test_bandwidth(args: &TestArgs) -> Result<()> {
    section("BANDWIDTH  (network pipe: this machine → Azure SQL)");

    let pool = build_pool(&args.conn, 2).await
        .context("could not connect for bandwidth test")?;

    // Pick the largest table for a meaningful sample
    let bundle = schema::extract(&pool, &[]).await?;
    let table = bundle.tables.iter()
        .max_by_key(|t| t.name.len()) // rough proxy; replaced below with row count
        .cloned()
        .unwrap_or_else(|| bundle.tables[0].clone());

    // Find actual largest by partition stats
    let mut best = bundle.tables[0].clone();
    let mut best_rows: i64 = 0;
    {
        let mut conn = pool.get().await?;
        for t in &bundle.tables {
            let sql = format!(
                "SELECT ISNULL(SUM(p.rows),0) FROM sys.partitions p \
                 JOIN sys.tables tbl ON tbl.object_id = p.object_id \
                 JOIN sys.schemas s ON s.schema_id = tbl.schema_id \
                 WHERE s.name='{}' AND tbl.name='{}' AND p.index_id IN (0,1)",
                t.schema, t.name
            );
            let r: i64 = conn.query(&sql, &[]).await?
                .into_results().await?
                .into_iter().flatten().next()
                .and_then(|row| row.get::<i64, _>(0))
                .unwrap_or(0);
            if r > best_rows { best_rows = r; best = t.clone(); }
        }
    }
    let _ = table; // replaced by best
    let table = best;

    let limit = args.bandwidth_rows.min(best_rows as u64);
    let select_cols: Vec<String> = table.columns.iter().take(5).map(|c|
        select_expr(&c.name, &c.sql_type).unwrap_or_else(|| format!("[{}]", c.name))
    ).collect();
    let query = format!(
        "SELECT TOP({limit}) {} FROM {} WITH (NOLOCK)",
        select_cols.join(", "), table.fqn
    );

    ok("table",       &table.fqn);
    ok("sample rows", &format!("{limit}"));

    let mut conn = pool.get().await?;
    let t0 = Instant::now();
    let stream = conn.query(&query, &[]).await?.into_row_stream();
    futures::pin_mut!(stream);

    let mut rows: u64 = 0;
    let mut approx_bytes: u64 = 0;
    while let Some(r) = stream.next().await {
        let row = r?;
        approx_bytes += row.len() as u64 * 12;
        rows += 1;
    }
    let elapsed = t0.elapsed();
    let rows_per_sec = rows as f64 / elapsed.as_secs_f64().max(0.001);
    let mb_per_sec   = approx_bytes as f64 / elapsed.as_secs_f64().max(0.001) / 1_048_576.0;

    ok("rows received", &format!("{rows}"));
    ok("elapsed",       &format!("{:.2}s", elapsed.as_secs_f64()));
    ok("rows/sec",      &format!("{rows_per_sec:.0}"));

    let pass = rows_per_sec >= 50_000.0;
    if pass {
        ok("throughput", &format!("~{mb_per_sec:.1} MB/s  — network is healthy"));
    } else {
        warn("throughput", &format!("~{mb_per_sec:.1} MB/s"), "high WAN latency detected — consider running from Azure VM in same region");
    }

    verdict(pass, &format!("{rows_per_sec:.0} rows/s single-connection bandwidth"));
    Ok(())
}

// ── source test ───────────────────────────────────────────────────────────────

async fn test_source(args: &TestArgs) -> Result<()> {
    section("SOURCE  (Azure SQL: connectivity, latency, table inventory)");

    let t_connect = Instant::now();
    let pool = build_pool(&args.conn, 4).await
        .context("could not connect — check host, credentials, and firewall")?;
    let connect_ms = t_connect.elapsed().as_millis();

    if connect_ms < 500 {
        ok("connect latency", &format!("{connect_ms}ms"));
    } else {
        warn("connect latency", &format!("{connect_ms}ms"), "high — check firewall/region");
    }

    let mut conn = pool.get().await?;

    // Ping latency: round-trip for a trivial query
    let t_ping = Instant::now();
    conn.query("SELECT 1", &[]).await?.into_results().await?;
    let ping_ms = t_ping.elapsed().as_millis();

    if ping_ms < 50 {
        ok("query RTT", &format!("{ping_ms}ms"));
    } else {
        warn("query RTT", &format!("{ping_ms}ms"), "consider running from Azure VM in same region");
    }

    // Service tier
    let tier_rows = conn.query(
        "SELECT TOP(1) service_objective FROM sys.database_service_objectives", &[]
    ).await?.into_results().await?;
    let tier = tier_rows.into_iter().flatten().next()
        .and_then(|r| r.get::<&str, _>(0).map(|s| s.to_string()))
        .unwrap_or_else(|| "unknown".to_string());
    ok("service tier", &tier);

    // Table inventory
    let bundle = schema::extract(&pool, &[]).await?;
    ok("tables found", &format!("{}", bundle.tables.len()));

    let mut total_rows: i64 = 0;
    let mut large_tables = 0usize;
    for t in &bundle.tables {
        let sql = format!(
            "SELECT ISNULL(SUM(p.rows),0) FROM sys.partitions p \
             JOIN sys.tables tbl ON tbl.object_id = p.object_id \
             JOIN sys.schemas s ON s.schema_id = tbl.schema_id \
             WHERE s.name='{}' AND tbl.name='{}' AND p.index_id IN (0,1)",
            t.schema, t.name
        );
        let r: i64 = conn.query(&sql, &[]).await?
            .into_results().await?
            .into_iter().flatten().next()
            .and_then(|row| row.get::<i64, _>(0))
            .unwrap_or(0);
        total_rows += r;
        if r > 500_000 { large_tables += 1; }
    }
    ok("total rows (est)", &format!("{total_rows}"));
    ok("large tables (>500k rows)", &format!("{large_tables}  — each will be chunked"));

    // Max DOP check
    let dop_rows = conn.query(
        "SELECT value_in_use FROM sys.configurations WHERE name = 'max degree of parallelism'", &[]
    ).await?.into_results().await?;
    let dop: i32 = dop_rows.into_iter().flatten().next()
        .and_then(|r| r.get::<i32, _>(0))
        .unwrap_or(0);
    if dop == 0 {
        ok("max DOP", "auto (server decides per query)");
    } else {
        ok("max DOP", &format!("{dop}"));
    }

    let pass = ping_ms < 200;
    verdict(pass, &format!(
        "{} tables, ~{} rows, {}ms RTT",
        bundle.tables.len(), total_rows, ping_ms
    ));
    Ok(())
}

// ── entry point ───────────────────────────────────────────────────────────────

pub async fn run(args: TestArgs) -> Result<()> {
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║             sqlrustler connection test                   ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("  target : {}:{}", args.conn.host, args.conn.database);
    println!("  testing: {}", args.only);

    if args.test_destination() {
        tokio::task::spawn_blocking({
            let rows = args.destination_rows;
            move || test_destination(rows)
        }).await??;
    }

    if args.test_bandwidth() {
        test_bandwidth(&args).await?;
    }

    if args.test_source() {
        test_source(&args).await?;
    }

    println!("\n  Done. Use `sqlrustler pressure` for deep ramp/saturation testing.\n");
    Ok(())
}
