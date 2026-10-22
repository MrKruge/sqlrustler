//! Pressure tester — finds the bottleneck in each layer of the export pipeline.
//!
//! Three phases, each run to saturation:
//!
//!   Phase 1 — Local Parquet sink
//!     Generates synthetic rows in memory and writes them to a temp Parquet file.
//!     No network. Measures peak local MB/s and CPU pressure.
//!     Ceiling: how fast this machine can encode + zstd-compress Arrow data.
//!
//!   Phase 2 — Network pipe probe
//!     Opens a single connection, streams all rows from the target table.
//!     Measures raw TDS receive throughput (rows/s, MB/s).
//!     Ceiling: WAN bandwidth between this machine and Azure SQL.
//!
//!   Phase 3 — DB ramp (concurrency sweep)
//!     Ramps from 1 → max_connections concurrent streaming queries against the
//!     same table. At each step, measures aggregate rows/s and per-connection
//!     rows/s. Reports the knee: the concurrency level where adding more
//!     connections stops improving throughput (server or network saturated).

use anyhow::{Context, Result};
use futures::StreamExt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use crate::cli::PressureArgs;
use crate::connection::build_pool;
use crate::schema;
use crate::types::select_expr;

// ── helpers ───────────────────────────────────────────────────────────────────

fn header(title: &str) {
    println!("\n{}", "─".repeat(60));
    println!("  {title}");
    println!("{}", "─".repeat(60));
}

fn result_row(label: &str, value: &str) {
    println!("  {:<30} {}", label, value);
}

// ── Phase 1: local Parquet sink ───────────────────────────────────────────────

pub fn run_local_sink(rows: usize, cols: usize, compression_level: i32) -> Result<LocalSinkResult> {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let schema = Arc::new(Schema::new(
        (0..cols)
            .map(|i| {
                if i % 2 == 0 {
                    Field::new(format!("col_int_{i}"), DataType::Int64, true)
                } else {
                    Field::new(format!("col_str_{i}"), DataType::Utf8, true)
                }
            })
            .collect::<Vec<_>>(),
    ));

    let tmp = tempfile::NamedTempFile::new()?;
    let file = std::fs::File::create(tmp.path())?;

    let compression = match compression_level {
        1 => Compression::ZSTD(parquet::basic::ZstdLevel::try_new(1)?),
        _  => Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3)?),
    };
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let batch_size = 100_000usize;
    let mut total_rows = 0usize;
    let t0 = Instant::now();

    while total_rows < rows {
        let n = batch_size.min(rows - total_rows);
        let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
        for i in 0..cols {
            if i % 2 == 0 {
                let vals: Int64Array = (0..n as i64).collect();
                arrays.push(Arc::new(vals));
            } else {
                let vals: StringArray = (0..n).map(|j| Some(format!("val_{j}"))).collect();
                arrays.push(Arc::new(vals));
            }
        }
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;
        writer.write(&batch)?;
        total_rows += n;
    }
    writer.close()?;

    let elapsed = t0.elapsed();
    let file_size = std::fs::metadata(tmp.path())?.len();

    Ok(LocalSinkResult {
        rows,
        cols,
        elapsed,
        output_bytes: file_size,
    })
}

pub struct LocalSinkResult {
    pub rows: usize,
    pub cols: usize,
    pub elapsed: Duration,
    pub output_bytes: u64,
}

impl LocalSinkResult {
    fn rows_per_sec(&self) -> f64 {
        self.rows as f64 / self.elapsed.as_secs_f64().max(0.001)
    }
    fn mb_per_sec(&self) -> f64 {
        self.output_bytes as f64 / self.elapsed.as_secs_f64().max(0.001) / 1_048_576.0
    }
}

// ── Phase 2: network probe ────────────────────────────────────────────────────

pub struct NetworkProbeResult {
    pub table_fqn: String,
    pub rows: u64,
    pub elapsed: Duration,
    pub approx_bytes: u64,
}

impl NetworkProbeResult {
    fn rows_per_sec(&self) -> f64 {
        self.rows as f64 / self.elapsed.as_secs_f64().max(0.001)
    }
    fn mb_per_sec(&self) -> f64 {
        self.approx_bytes as f64 / self.elapsed.as_secs_f64().max(0.001) / 1_048_576.0
    }
}

async fn probe_network(
    pool: &crate::connection::DbPool,
    table_fqn: &str,
    select_list: &str,
    batch_size: usize,
    max_rows: Option<u64>,
) -> Result<NetworkProbeResult> {
    let mut conn = pool.get().await.context("failed to get connection for network probe")?;
    let top = max_rows.map(|n| format!("TOP ({n}) ")).unwrap_or_default();
    let query = format!("SELECT {top}{select_list} FROM {table_fqn} WITH (NOLOCK)");

    let t0 = Instant::now();
    let stream = conn.query(&query, &[]).await?.into_row_stream();
    futures::pin_mut!(stream);

    let mut rows: u64 = 0;
    let mut approx_bytes: u64 = 0;
    let mut batch: Vec<tiberius::Row> = Vec::with_capacity(batch_size);

    while let Some(row_result) = stream.next().await {
        let row = row_result?;
        // rough byte estimate: count non-null column values × average bytes per value
        approx_bytes += row.len() as u64 * 12;
        batch.push(row);
        rows += 1;
        if batch.len() >= batch_size {
            batch.clear();
        }
    }

    Ok(NetworkProbeResult {
        table_fqn: table_fqn.to_string(),
        rows,
        elapsed: t0.elapsed(),
        approx_bytes,
    })
}

// ── Phase 3: DB ramp ──────────────────────────────────────────────────────────

pub struct RampStep {
    pub connections: usize,
    pub aggregate_rows_per_sec: f64,
    pub per_conn_rows_per_sec: f64,
    pub elapsed: Duration,
}

async fn ramp_step(
    conn_args: &crate::cli::ConnectArgs,
    table_fqn: &str,
    select_list: &str,
    connections: usize,
    step_secs: u64,
) -> Result<RampStep> {
    // Workers drop their row streams mid-result at the deadline; those connections must not
    // be reused, so each step gets its own pool.
    let pool = crate::connection::build_pool(conn_args, connections as u32 + 1).await?;
    // Each worker streams rows from the table for exactly step_secs seconds,
    // then reports how many rows it read. All workers start simultaneously.
    let deadline = Instant::now() + Duration::from_secs(step_secs);
    let sem = Arc::new(Semaphore::new(connections));
    let mut handles = Vec::new();

    for _ in 0..connections {
        let pool = pool.clone();
        let fqn = table_fqn.to_string();
        let sel = select_list.to_string();
        let deadline = deadline;
        let sem = sem.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await?;
            let mut conn = pool.get().await?;
            let query = format!("SELECT {sel} FROM {fqn} WITH (NOLOCK)");
            let stream = conn.query(&query, &[]).await?.into_row_stream();
            futures::pin_mut!(stream);

            let mut rows: u64 = 0;
            while Instant::now() < deadline {
                match tokio::time::timeout(
                    Duration::from_secs(5),
                    stream.next(),
                ).await {
                    Ok(Some(r)) => { r?; rows += 1; }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            anyhow::Ok(rows)
        }));
    }

    let t0 = Instant::now();
    let mut total_rows: u64 = 0;
    for h in handles {
        total_rows += h.await??;
    }
    let elapsed = t0.elapsed();
    let agg_rps = total_rows as f64 / elapsed.as_secs_f64().max(0.001);

    Ok(RampStep {
        connections,
        aggregate_rows_per_sec: agg_rps,
        per_conn_rows_per_sec: agg_rps / connections as f64,
        elapsed,
    })
}

// ── entry point ───────────────────────────────────────────────────────────────

pub async fn run(args: PressureArgs) -> Result<()> {
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║              sqlrustler pressure tester                  ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!("  target : {}:{}", args.conn.host, args.conn.database);
    println!("  ramp   : 1 → {} connections, {}s per step", args.max_connections, args.step_secs);

    // ── Phase 1: local sink ───────────────────────────────────────────────────
    if !args.skip_local {
        header("Phase 1 — Local Parquet sink  (no network)");
        println!("  Writing 5M synthetic rows × 10 cols to temp Parquet file...");

        let result = tokio::task::spawn_blocking(|| {
            run_local_sink(5_000_000, 10, 3)
        }).await??;

        result_row("rows written",   &format!("{}", result.rows));
        result_row("elapsed",        &format!("{:.2}s", result.elapsed.as_secs_f64()));
        result_row("rows/sec",       &format!("{:.0}", result.rows_per_sec()));
        result_row("output size",    &bytesize::ByteSize(result.output_bytes).to_string());
        result_row("write MB/s",     &format!("{:.1} MB/s", result.mb_per_sec()));
        println!("\n  → This is your LOCAL CEILING. Network phases cannot exceed this.");
    }

    // ── Connect ───────────────────────────────────────────────────────────────
    let max_pool = (args.max_connections + 4) as u32;
    tracing::info!("Connecting to {}/{} (pool {})", args.conn.host, args.conn.database, max_pool);
    let pool = build_pool(&args.conn, max_pool).await?;

    // Resolve target table
    let bundle = schema::extract(&pool, &[]).await?;
    let table = if let Some(ref fqn) = args.table {
        bundle.tables.iter()
            .find(|t| t.fqn.eq_ignore_ascii_case(fqn))
            .cloned()
            .with_context(|| format!("Table '{fqn}' not found in database"))?
    } else {
        // Pick largest table by partition row estimate
        let mut conn = pool.get().await?;
        let mut best = bundle.tables[0].clone();
        let mut best_rows: i64 = 0;
        for t in &bundle.tables {
            let sql = format!(
                "SELECT ISNULL(SUM(p.rows),0) FROM sys.partitions p \
                 JOIN sys.tables tbl ON tbl.object_id = p.object_id \
                 JOIN sys.schemas s ON s.schema_id = tbl.schema_id \
                 WHERE s.name = '{}' AND tbl.name = '{}' AND p.index_id IN (0,1)",
                t.schema, t.name
            );
            let r: i64 = conn.query(&sql, &[]).await?
                .into_results().await?
                .into_iter().flatten().next()
                .and_then(|row| row.get::<i64, _>(0))
                .unwrap_or(0);
            if r > best_rows {
                best_rows = r;
                best = t.clone();
            }
        }
        tracing::info!("Auto-selected largest table: {} ({} rows)", best.fqn, best_rows);
        best
    };

    // Lean SELECT list by default (first 5 cols); --columns overrides
    let chosen: Vec<&crate::schema::ColumnInfo> = match args.columns.as_deref() {
        None => table.columns.iter().take(5).collect(),
        Some(s) if s.eq_ignore_ascii_case("all") => table.columns.iter().collect(),
        Some(s) => {
            let mut v = Vec::new();
            for name in s.split(',').map(str::trim).filter(|n| !n.is_empty()) {
                v.push(
                    table.columns.iter()
                        .find(|c| c.name.eq_ignore_ascii_case(name))
                        .with_context(|| format!("Column '{name}' not found in {}", table.fqn))?,
                );
            }
            v
        }
    };
    let select_cols: Vec<String> = chosen.iter().map(|c| {
        select_expr(&c.name, &c.sql_type)
            .unwrap_or_else(|| format!("[{}]", c.name))
    }).collect();
    let select_list = select_cols.join(", ");
    println!("  columns: {}", chosen.iter().map(|c| c.name.as_str()).collect::<Vec<_>>().join(", "));

    // ── Phase 2: network probe ────────────────────────────────────────────────
    if !args.skip_network {
        header(&format!("Phase 2 — Network pipe  (table: {})", table.fqn));
        println!("  Streaming all rows over a single connection. Ctrl-C to abort.");

        let result = probe_network(&pool, &table.fqn, &select_list, args.batch_size, args.max_rows).await?;

        result_row("table",        &table.fqn);
        result_row("rows received", &format!("{}", result.rows));
        result_row("elapsed",       &format!("{:.2}s", result.elapsed.as_secs_f64()));
        result_row("rows/sec",      &format!("{:.0}", result.rows_per_sec()));
        result_row("approx MB/s",   &format!("{:.1} MB/s  (estimated from col count)", result.mb_per_sec()));
        println!("\n  → This is your NETWORK CEILING from this machine.");
        println!("    Run from an Azure VM in the same region for 10-100× more.");
    }

    // ── Phase 3: DB ramp ──────────────────────────────────────────────────────
    if !args.skip_db_ramp {
        header(&format!("Phase 3 — DB concurrency ramp  (table: {})", table.fqn));
        println!("  Ramping connections 1 → {}. Each step runs for {}s.", args.max_connections, args.step_secs);
        println!("  Watch for rows/sec to plateau — that's your Azure SQL ceiling.\n");
        println!("  {:>6}  {:>14}  {:>14}  {:>10}", "conns", "agg rows/s", "per-conn r/s", "elapsed");
        println!("  {}", "─".repeat(50));

        let steps: Vec<usize> = {
            let mut v = vec![1, 2, 4];
            let mut n = 8;
            while n <= args.max_connections {
                v.push(n);
                n *= 2;
            }
            if *v.last().unwrap() != args.max_connections {
                v.push(args.max_connections);
            }
            v
        };

        let mut prev_agg: f64 = 0.0;
        let mut knee: Option<usize> = None;

        for &conns in &steps {
            let step = ramp_step(&args.conn, &table.fqn, &select_list, conns, args.step_secs).await?;

            let improvement = if prev_agg > 0.0 {
                ((step.aggregate_rows_per_sec - prev_agg) / prev_agg) * 100.0
            } else {
                100.0
            };

            let flag = if improvement < 10.0 && knee.is_none() && conns > 1 {
                knee = Some(conns);
                "  ← KNEE (diminishing returns)"
            } else {
                ""
            };

            println!(
                "  {:>6}  {:>14.0}  {:>14.0}  {:>10.2}s  ({:+.0}%){flag}",
                conns,
                step.aggregate_rows_per_sec,
                step.per_conn_rows_per_sec,
                step.elapsed.as_secs_f64(),
                improvement,
            );

            prev_agg = step.aggregate_rows_per_sec;
        }

        println!("\n  Summary:");
        if let Some(k) = knee {
            println!("  → Optimal concurrency: {k} connections");
            println!("    Beyond this, Azure SQL is the bottleneck (DTU/vCore limit).");
            println!("    Set --parallel {k} for best real-world export throughput.");
        } else {
            println!("  → No knee detected up to {} connections.", args.max_connections);
            println!("    Try --max-connections {} to find the ceiling.", args.max_connections * 2);
        }
    }

    println!("\n{}", "─".repeat(60));
    println!("  Pressure test complete.");
    println!("{}\n", "─".repeat(60));

    Ok(())
}
