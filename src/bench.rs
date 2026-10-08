use anyhow::Result;
use std::time::Instant;

use crate::cli::{BenchArgs, ExportArgs};
use crate::export;

pub async fn run(args: BenchArgs) -> Result<()> {
    let output = args.output.clone().unwrap_or_else(|| {
        format!(
            "bench_{}.sqlrustler",
            chrono::Utc::now().format("%Y%m%dT%H%M%S")
        )
        .into()
    });

    let export_args = ExportArgs {
        conn: args.conn.clone(),
        output: Some(output.clone()),
        parallel: num_cpus::get(),
        batch_size: 10_000,
        exclude_tables: vec![],
        schema_only: false,
        compression_level: 3,
    };

    // ── sqlrustler timing ─────────────────────────────────────────────────────────
    tracing::info!("Starting sqlrustler export benchmark");
    let t0 = Instant::now();
    export::run(export_args).await?;
    let sqlrustler_elapsed = t0.elapsed();
    let sqlrustler_size = std::fs::metadata(&output)?.len();

    println!("\n=== sqlrustler benchmark results ===");
    println!("  wall time : {:.2}s", sqlrustler_elapsed.as_secs_f64());
    println!("  file size : {}", bytesize::ByteSize(sqlrustler_size));
    println!(
        "  throughput: {}/s",
        bytesize::ByteSize(
            (sqlrustler_size as f64 / sqlrustler_elapsed.as_secs_f64().max(0.001)) as u64
        )
    );

    // ── sqlpackage comparison ─────────────────────────────────────────────────
    if let Some(compare) = &args.compare {
        if compare == "sqlpackage" {
            run_sqlpackage_comparison(&args, sqlrustler_elapsed, sqlrustler_size).await?;
        }
    }

    Ok(())
}

async fn run_sqlpackage_comparison(
    args: &BenchArgs,
    sqlrustler_elapsed: std::time::Duration,
    sqlrustler_size: u64,
) -> Result<()> {
    let sqlpackage_path = which::which("sqlpackage").map_err(|_| {
        anyhow::anyhow!(
            "sqlpackage not found on PATH.\n\
             Install: https://learn.microsoft.com/en-us/sql/tools/sqlpackage/sqlpackage-download"
        )
    })?;

    let bacpac_out = tempfile::NamedTempFile::new()?;
    let conn = &args.conn;

    let mut sp_args = vec![
        "/Action:Export".to_string(),
        format!("/TargetFile:{}", bacpac_out.path().display()),
        format!("/SourceServerName:{},{}", conn.host, conn.port),
        format!("/SourceDatabaseName:{}", conn.database),
    ];

    if let Some(user) = &conn.user {
        sp_args.push(format!("/SourceUser:{user}"));
    }
    if let Some(pass) = &conn.password {
        sp_args.push(format!("/SourcePassword:{pass}"));
    }

    tracing::info!("Starting sqlpackage export benchmark");
    let t0 = Instant::now();
    let status = tokio::process::Command::new(&sqlpackage_path)
        .args(&sp_args)
        .status()
        .await?;
    let sp_elapsed = t0.elapsed();

    if !status.success() {
        tracing::warn!("sqlpackage exited with non-zero status: {status}");
    }

    let sp_size = std::fs::metadata(bacpac_out.path())
        .map(|m| m.len())
        .unwrap_or(0);

    // ── Side-by-side table ────────────────────────────────────────────────────
    println!("\n=== Benchmark comparison ===");
    println!(
        "{:<15} {:>10} {:>12} {:>12}",
        "tool", "wall(s)", "file_size", "throughput"
    );
    println!("{:-<51}", "");

    let fmt_row = |name: &str, elapsed: std::time::Duration, size: u64| {
        println!(
            "{:<15} {:>10.2} {:>12} {:>12}",
            name,
            elapsed.as_secs_f64(),
            bytesize::ByteSize(size).to_string(),
            format!(
                "{}/s",
                bytesize::ByteSize(
                    (size as f64 / elapsed.as_secs_f64().max(0.001)) as u64
                )
            ),
        )
    };

    fmt_row("sqlrustler", sqlrustler_elapsed, sqlrustler_size);
    fmt_row("sqlpackage", sp_elapsed, sp_size);

    let speedup = sp_elapsed.as_secs_f64() / sqlrustler_elapsed.as_secs_f64().max(0.001);
    println!("\n  speedup: {speedup:.1}x (sqlrustler vs sqlpackage)");

    Ok(())
}
