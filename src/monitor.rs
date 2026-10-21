//! Four-agent live monitoring harness.
//!
//! Spawns four concurrent tasks during an export:
//!
//!   Agent 1 — Job         : the export itself (managed by caller)
//!   Agent 2 — Local       : CPU %, memory used, local disk write MB/s
//!   Agent 3 — Network     : bytes/s received on the primary interface (WAN pipe)
//!   Agent 4 — Azure SQL   : DTU %, CPU %, active connections via Azure Monitor CLI
//!
//! All agents sample on the same interval and write to a shared timeline.
//! When the export completes, call `MonitorHandle::finish()` to get the report.

use anyhow::Result;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

use crate::connection::DbPool;

// ── Sample types ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct Sample {
    pub elapsed_secs: f64,
    pub local:   LocalSample,
    pub network: NetworkSample,
    pub azure:   Option<AzureSample>,
}

#[derive(Clone, Debug, Default)]
pub struct LocalSample {
    pub cpu_pct:    f64,   // 0–100
    pub mem_used_mb: f64,
    pub mem_total_mb: f64,
}

#[derive(Clone, Debug, Default)]
pub struct NetworkSample {
    pub rx_mbps: f64,  // megabytes/s received
    pub tx_mbps: f64,  // megabytes/s sent
}

#[derive(Clone, Debug, Default)]
pub struct AzureSample {
    pub cpu_pct:            f64,   // cpu_percent
    pub workers_pct:        f64,   // workers_percent
    pub connections:        i64,   // connection_successful (new conns/min)
    pub data_read_pct:      f64,   // physical_data_read_percent
}

// ── Shared timeline ───────────────────────────────────────────────────────────

pub type Timeline = Arc<Mutex<Vec<Sample>>>;

pub struct MonitorHandle {
    timeline:  Timeline,
    stop_tx:   tokio::sync::watch::Sender<bool>,
    local_h:   JoinHandle<()>,
    network_h: JoinHandle<()>,
    azure_h:   JoinHandle<()>,
    start:     Instant,
    pub resource_id: Arc<tokio::sync::Mutex<Option<String>>>,
}

impl MonitorHandle {
    /// Signal all agents to stop and collect the final timeline.
    pub async fn finish(self) -> Vec<Sample> {
        let _ = self.stop_tx.send(true);
        let _ = tokio::join!(self.local_h, self.network_h, self.azure_h);
        let samples = self.timeline.lock().unwrap().clone();
        samples
    }
}

// ── Agent 2: local metrics (macOS) ───────────────────────────────────────────

fn sample_local() -> LocalSample {
    // CPU: parse `top -l 2 -s 1 -n 0` — first sample is since boot, second is 1s window
    let cpu_pct = std::process::Command::new("sh")
        .args(["-c", "top -l 2 -s 1 -n 0 | awk '/CPU usage/ {last=$0} END {gsub(/%.*/, \"\", last); split(last, a, \": \"); split(a[2], b, \",\"); gsub(/ %.*/, \"\", b[1]); print b[1]}'"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<f64>().ok())
        .unwrap_or(0.0);

    // Memory: vm_stat gives pages; page size is 16384 bytes on Apple Silicon, 4096 on Intel
    let (mem_used_mb, mem_total_mb) = parse_vmstat().unwrap_or((0.0, 0.0));

    LocalSample { cpu_pct, mem_used_mb, mem_total_mb }
}

fn parse_vmstat() -> Option<(f64, f64)> {
    let out = std::process::Command::new("vm_stat").output().ok()?;
    let text = String::from_utf8(out.stdout).ok()?;

    // Page size from first line: "Mach Virtual Memory Statistics: (page size of 16384 bytes)"
    let page_bytes: u64 = text.lines()
        .next()?
        .split("page size of ")
        .nth(1)?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;

    let parse_pages = |key: &str| -> u64 {
        text.lines()
            .find(|l| l.contains(key))
            .and_then(|l| l.split(':').nth(1))
            .map(|v| v.trim().trim_end_matches('.'))
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };

    let active   = parse_pages("Pages active");
    let wired    = parse_pages("Pages wired down");
    let occupied = parse_pages("Pages occupied by compressor");
    let free     = parse_pages("Pages free");
    let speculative = parse_pages("Pages speculative");

    let used_bytes  = (active + wired + occupied) * page_bytes;
    let total_bytes = (active + wired + occupied + free + speculative) * page_bytes;

    Some((
        used_bytes  as f64 / 1_048_576.0,
        total_bytes as f64 / 1_048_576.0,
    ))
}

// ── Agent 3: network metrics ──────────────────────────────────────────────────

fn sample_network_bytes() -> (u64, u64) {
    // netstat -ib gives cumulative byte counts per interface.
    // Sum across all en* interfaces (en0 = Wi-Fi, en1+ = Ethernet/Thunderbolt).
    let out = match std::process::Command::new("netstat").args(["-ib"]).output() {
        Ok(o) => o,
        Err(_) => return (0, 0),
    };
    let text = match String::from_utf8(out.stdout) {
        Ok(t) => t,
        Err(_) => return (0, 0),
    };

    let mut rx: u64 = 0;
    let mut tx: u64 = 0;

    for line in text.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        // netstat -ib columns: Name Mtu Network Address Ipkts Ierrs Ibytes Opkts Oerrs Obytes Drop
        // Only sum rows where Name starts with "en" and Network is not "<Link#>"
        if parts.len() < 10 { continue; }
        if !parts[0].starts_with("en") { continue; }
        if parts[2].starts_with('<') { continue; } // skip Link# rows, use IP rows
        rx = rx.saturating_add(parts[6].parse::<u64>().unwrap_or(0));
        tx = tx.saturating_add(parts[9].parse::<u64>().unwrap_or(0));
    }
    (rx, tx)
}

// ── Agent 4: Azure SQL metrics via az cli ────────────────────────────────────

async fn discover_resource_id(host: &str, database: &str, resource_group: Option<&str>) -> Option<String> {
    // Strip .database.windows.net to get server name
    let server_name = host.split('.').next()?;

    if let Some(rg) = resource_group {
        // Caller provided RG — build directly
        let sub = get_subscription_id().await?;
        return Some(format!(
            "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Sql/servers/{server_name}/databases/{database}"
        ));
    }

    // Auto-discover via az sql server list
    let out = tokio::process::Command::new("az")
        .args(["sql", "server", "list",
               "--query", &format!("[?name=='{}'].resourceGroup", server_name),
               "-o", "tsv"])
        .output().await.ok()?;
    let rg = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if rg.is_empty() { return None; }

    let sub = get_subscription_id().await?;
    Some(format!(
        "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Sql/servers/{server_name}/databases/{database}"
    ))
}

async fn get_subscription_id() -> Option<String> {
    let out = tokio::process::Command::new("az")
        .args(["account", "show", "--query", "id", "-o", "tsv"])
        .output().await.ok()?;
    let id = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if id.is_empty() { None } else { Some(id) }
}

async fn fetch_azure_sample(resource_id: &str) -> Option<AzureSample> {
    // Valid metrics for Microsoft.Sql/servers/databases confirmed via az monitor metrics list.
    // Note: dtu_consumption_percent does NOT exist as an Azure Monitor metric.
    let metrics = "cpu_percent,workers_percent,connection_successful,physical_data_read_percent";
    let out = tokio::process::Command::new("az")
        .args([
            "monitor", "metrics", "list",
            "--resource", resource_id,
            "--metric", metrics,
            "--interval", "PT1M",
            "--aggregation", "Average",
            "--output", "json",
        ])
        .output().await.ok()?;

    let text = String::from_utf8(out.stdout).ok()?;
    let json: serde_json::Value = serde_json::from_str(&text).ok()?;

    let get_latest = |metric_name: &str| -> f64 {
        (|| -> Option<f64> {
            json["value"].as_array()?
                .iter()
                .find(|m| m["name"]["value"].as_str() == Some(metric_name))?
                ["timeseries"][0]["data"]
                .as_array()?
                .iter()
                .rev()
                .find_map(|p| p["average"].as_f64())
        })().unwrap_or(0.0)
    };

    Some(AzureSample {
        cpu_pct:       get_latest("cpu_percent"),
        workers_pct:   get_latest("workers_percent"),
        connections:   get_latest("connection_successful") as i64,
        data_read_pct: get_latest("physical_data_read_percent"),
    })
}

// ── Start all agents ─────────────────────────────────────────────────────────

pub async fn start(
    host: String,
    database: String,
    resource_group: Option<String>,
    interval_secs: u64,
) -> MonitorHandle {
    let timeline: Timeline = Arc::new(Mutex::new(Vec::new()));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let start = Instant::now();

    // Discover Azure resource ID in the background (may take a few seconds)
    let resource_id: Arc<tokio::sync::Mutex<Option<String>>> = Arc::new(tokio::sync::Mutex::new(None));
    {
        let rid = resource_id.clone();
        let h = host.clone();
        let db = database.clone();
        let rg = resource_group.clone();
        tokio::spawn(async move {
            if let Some(id) = discover_resource_id(&h, &db, rg.as_deref()).await {
                tracing::info!("Azure Monitor resource ID discovered: {}", id);
                *rid.lock().await = Some(id);
            } else {
                tracing::warn!("Could not discover Azure Monitor resource ID — Azure metrics will be unavailable.\
                    \n  Pass --azure-resource-group <rg> to enable them.");
            }
        });
    }

    // ── Agent 2: local ───────────────────────────────────────────────────────
    let local_h = {
        let tl = timeline.clone();
        let mut rx = stop_rx.clone();
        let start = start.clone();
        tokio::task::spawn_blocking(move || {
            loop {
                if *rx.borrow() { break; }
                let s = sample_local();
                tl.lock().unwrap().last_mut().map(|last| last.local = s);
                std::thread::sleep(Duration::from_secs(interval_secs));
            }
        })
    };

    // ── Agent 3: network ─────────────────────────────────────────────────────
    let network_h = {
        let tl = timeline.clone();
        let mut rx = stop_rx.clone();
        tokio::task::spawn_blocking(move || {
            let (mut prev_rx, mut prev_tx) = sample_network_bytes();
            std::thread::sleep(Duration::from_secs(interval_secs));
            loop {
                if *rx.borrow() { break; }
                let (cur_rx, cur_tx) = sample_network_bytes();
                let rx_mbps = (cur_rx.saturating_sub(prev_rx)) as f64 / interval_secs as f64 / 1_048_576.0;
                let tx_mbps = (cur_tx.saturating_sub(prev_tx)) as f64 / interval_secs as f64 / 1_048_576.0;
                prev_rx = cur_rx; prev_tx = cur_tx;
                tl.lock().unwrap().last_mut().map(|last| last.network = NetworkSample { rx_mbps, tx_mbps });
                std::thread::sleep(Duration::from_secs(interval_secs));
            }
        })
    };

    // ── Agent 4: azure ───────────────────────────────────────────────────────
    let azure_h = {
        let tl = timeline.clone();
        let mut rx = stop_rx.clone();
        let rid = resource_id.clone();
        // Azure Monitor has 1-min granularity — sample every 30s for freshness
        let azure_interval = interval_secs.max(30);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(azure_interval));
            loop {
                interval.tick().await;
                if *rx.borrow() { break; }
                let maybe_id = rid.lock().await.clone();
                if let Some(ref id) = maybe_id {
                    if let Some(s) = fetch_azure_sample(id).await {
                        tl.lock().unwrap().last_mut().map(|last| last.azure = Some(s));
                    }
                }
            }
        })
    };

    // ── Ticker: push a new sample slot every interval ────────────────────────
    {
        let tl = timeline.clone();
        let mut rx = stop_rx.clone();
        let start = start.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            loop {
                interval.tick().await;
                if *rx.borrow() { break; }
                tl.lock().unwrap().push(Sample {
                    elapsed_secs: start.elapsed().as_secs_f64(),
                    local:   LocalSample::default(),
                    network: NetworkSample::default(),
                    azure:   None,
                });
            }
        });
    }

    MonitorHandle { timeline, stop_tx, local_h, network_h, azure_h, start, resource_id }
}

// ── Report printer ────────────────────────────────────────────────────────────

pub fn print_report(samples: &[Sample]) {
    if samples.is_empty() {
        println!("  (no monitor samples collected)");
        return;
    }

    let has_azure = samples.iter().any(|s| s.azure.is_some());

    println!("\n╔══════════════════════════════════════════════════════════════════════════╗");
    println!("║                     LIVE MONITOR REPORT                                ║");
    println!("╚══════════════════════════════════════════════════════════════════════════╝");

    if has_azure {
        println!("\n  {:>6}  {:>7}  {:>8}  {:>8}  {:>8}  {:>8}  {:>10}  {:>8}",
            "t(s)", "cpu%", "mem_gb", "net_rx", "az_cpu%", "az_wrkr%", "az_conns", "az_io%");
        println!("  {}", "─".repeat(74));
    } else {
        println!("\n  {:>6}  {:>7}  {:>8}  {:>8}",
            "t(s)", "cpu%", "mem_gb", "net_rx");
        println!("  {}", "─".repeat(38));
    }

    for s in samples {
        let mem_gb = s.local.mem_used_mb / 1024.0;
        let net = format!("{:.1}MB/s", s.network.rx_mbps);

        if has_azure {
            let (cpu, wrk, conns, io) = s.azure.as_ref()
                .map(|a| (a.cpu_pct, a.workers_pct, a.connections, a.data_read_pct))
                .unwrap_or((0.0, 0.0, 0, 0.0));
            println!("  {:>6.0}  {:>6.1}%  {:>7.2}G  {:>8}  {:>7.1}%  {:>7.1}%  {:>10}  {:>7.1}%",
                s.elapsed_secs, s.local.cpu_pct, mem_gb, net, cpu, wrk, conns, io);
        } else {
            println!("  {:>6.0}  {:>6.1}%  {:>7.2}G  {:>8}",
                s.elapsed_secs, s.local.cpu_pct, mem_gb, net);
        }
    }

    // Summary
    let avg_cpu  = samples.iter().map(|s| s.local.cpu_pct).sum::<f64>() / samples.len() as f64;
    let peak_rx  = samples.iter().map(|s| s.network.rx_mbps).fold(0.0_f64, f64::max);
    let avg_rx   = samples.iter().map(|s| s.network.rx_mbps).sum::<f64>() / samples.len() as f64;

    println!("\n  Summary:");
    println!("    local avg CPU   : {avg_cpu:.1}%");
    println!("    network peak rx : {peak_rx:.2} MB/s");
    println!("    network avg rx  : {avg_rx:.2} MB/s");

    if has_azure {
        let az: Vec<&AzureSample> = samples.iter().filter_map(|s| s.azure.as_ref()).collect();
        if !az.is_empty() {
            let peak_az_cpu  = az.iter().map(|a| a.cpu_pct).fold(0.0_f64, f64::max);
            let avg_az_cpu   = az.iter().map(|a| a.cpu_pct).sum::<f64>() / az.len() as f64;
            let peak_workers = az.iter().map(|a| a.workers_pct).fold(0.0_f64, f64::max);
            let peak_io      = az.iter().map(|a| a.data_read_pct).fold(0.0_f64, f64::max);
            println!("    azure avg CPU   : {avg_az_cpu:.1}%");
            println!("    azure peak CPU  : {peak_az_cpu:.1}%");
            println!("    azure peak wrkr : {peak_workers:.1}%");
            println!("    azure peak I/O  : {peak_io:.1}%");

            // Bottleneck verdict — based only on measured metrics
            println!("\n  Bottleneck:");
            if peak_az_cpu > 80.0 {
                println!("    ⚠  Azure SQL CPU peaked at {peak_az_cpu:.0}% — server CPU is the ceiling.");
                println!("       Scale up service tier or reduce --table-parallelism.");
            } else if peak_workers > 80.0 {
                println!("    ⚠  Azure SQL workers peaked at {peak_workers:.0}% — connection limit hit.");
                println!("       Reduce --table-parallelism.");
            } else if peak_io > 80.0 {
                println!("    ⚠  Azure SQL physical I/O peaked at {peak_io:.0}% — storage throughput ceiling.");
            } else if avg_rx < 5.0 {
                println!("    ⚠  Network avg {avg_rx:.1} MB/s — WAN pipe may be the ceiling.");
                println!("       Run from an Azure VM in the same region for 10-100× speedup.");
            } else if avg_cpu > 60.0 {
                println!("    ⚠  Local CPU avg {avg_cpu:.0}% — this machine is the ceiling.");
            } else {
                println!("    ✓  No single layer saturated. Try increasing --table-parallelism.");
            }
        }
    } else {
        println!("\n  Bottleneck (local + network only — pass --monitor with --azure-resource-group for full picture):");
        if avg_rx < 5.0 {
            println!("    ⚠  Network avg {avg_rx:.1} MB/s — WAN pipe is the ceiling.");
        } else if avg_cpu > 60.0 {
            println!("    ⚠  Local CPU avg {avg_cpu:.0}%.");
        } else {
            println!("    ✓  Local machine and network look healthy. Azure SQL tier may be the limit.");
        }
    }

    println!();
}

// ── Query Store post-export report ────────────────────────────────────────────

/// Pull the top-10 slowest queries from Azure SQL Query Store and print a report.
///
/// This is purely observational — read-only, no writes to the source DB.
/// Requires Query Store to be enabled on the database (default on Azure SQL).
pub async fn query_store_report(pool: &DbPool) {
    match fetch_query_store(pool).await {
        Ok(rows) if rows.is_empty() => {
            println!("\n  (Query Store: no data — possibly not enabled or no queries captured)");
        }
        Ok(rows) => print_query_store_table(&rows),
        Err(e) => {
            tracing::debug!("Query Store report skipped: {e:#}");
            println!("\n  (Query Store report unavailable — pass --azure-auth or check permissions)");
        }
    }
}

struct QsRow {
    query_text: String,
    avg_duration_ms: f64,
    count_executions: i64,
    avg_logical_io_reads: f64,
    avg_cpu_time_ms: f64,
}

async fn fetch_query_store(pool: &DbPool) -> Result<Vec<QsRow>> {
    let mut conn = pool.get().await?;

    // Only look at queries that ran during the last 30 minutes (covers our export window).
    // avg_duration in Query Store is in microseconds.
    let sql = "
        SELECT TOP 10
            SUBSTRING(qt.query_sql_text, 1, 300)                       AS query_text,
            rs.avg_duration         / 1000.0                           AS avg_duration_ms,
            rs.count_executions,
            ISNULL(rs.avg_logical_io_reads, 0)                         AS avg_logical_io_reads,
            rs.avg_cpu_time         / 1000.0                           AS avg_cpu_time_ms
        FROM sys.query_store_query_text  AS qt
        JOIN sys.query_store_query       AS q  ON q.query_text_id = qt.query_text_id
        JOIN sys.query_store_plan        AS p  ON p.query_id      = q.query_id
        JOIN sys.query_store_runtime_stats AS rs ON rs.plan_id    = p.plan_id
        JOIN sys.query_store_runtime_stats_interval AS rsi
             ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
        WHERE rsi.start_time >= DATEADD(MINUTE, -30, GETUTCDATE())
          AND rs.count_executions >= 1
        ORDER BY rs.avg_duration DESC
    ";

    let result = conn.query(sql, &[]).await?.into_results().await?;
    let mut rows = Vec::new();
    for row in result.into_iter().flatten() {
        let text: &str = row.get(0).unwrap_or("");
        let avg_dur: f64 = row.get::<f64, _>(1).unwrap_or(0.0);
        let count: i64   = row.get::<i64, _>(2).unwrap_or(0);
        let lio: f64     = row.get::<f64, _>(3).unwrap_or(0.0);
        let cpu: f64     = row.get::<f64, _>(4).unwrap_or(0.0);
        rows.push(QsRow {
            query_text: text.trim().replace('\n', " ").replace('\r', ""),
            avg_duration_ms: avg_dur,
            count_executions: count,
            avg_logical_io_reads: lio,
            avg_cpu_time_ms: cpu,
        });
    }
    Ok(rows)
}

fn print_query_store_table(rows: &[QsRow]) {
    println!("\n╔══════════════════════════════════════════════════════════════════════════╗");
    println!("║                  AZURE SQL QUERY STORE (last 30 min)                   ║");
    println!("╚══════════════════════════════════════════════════════════════════════════╝");
    println!();
    println!("  {:<55}  {:>10}  {:>8}  {:>10}  {:>8}",
        "query (first 55 chars)", "avg_ms", "execs", "avg_lio", "avg_cpu_ms");
    println!("  {}", "─".repeat(97));

    for r in rows {
        let snippet: String = r.query_text.chars().take(55).collect();
        let flag = if r.avg_duration_ms > 5_000.0 { " ⚠" } else { "  " };
        println!("{flag} {:<55}  {:>10.1}  {:>8}  {:>10.0}  {:>8.1}",
            snippet, r.avg_duration_ms, r.count_executions, r.avg_logical_io_reads, r.avg_cpu_time_ms);
    }

    // Bottleneck hints
    let max_dur = rows.iter().map(|r| r.avg_duration_ms).fold(0.0_f64, f64::max);
    let max_lio = rows.iter().map(|r| r.avg_logical_io_reads).fold(0.0_f64, f64::max);

    println!("\n  Query Store insights:");
    if max_dur > 10_000.0 {
        println!("    ⚠  Slowest query avg {max_dur:.0}ms — index or service tier may be limiting.");
    }
    if max_lio > 100_000.0 {
        println!("    ⚠  Highest avg logical I/O reads {max_lio:.0} — heavy table scans in progress.");
    }
    if max_dur <= 10_000.0 && max_lio <= 100_000.0 {
        println!("    ✓  Query durations and I/O look healthy — bottleneck is likely network or DTU cap.");
    }
    println!();
}
