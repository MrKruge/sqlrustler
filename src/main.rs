use anyhow::Result;
use tracing_subscriber::EnvFilter;

mod archive;
mod bench;
mod cli;
mod connection;
mod export;
mod import;
mod parquet_writer;
mod progress;
mod schema;
mod tui;
mod types;

#[tokio::main]
async fn main() -> Result<()> {
    // If no subcommand is given, launch the interactive TUI.
    // The TUI collects all arguments itself and calls the right run function.
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 1 {
        return tui::run_tui().await;
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("sqlrustler=info")),
        )
        .with_target(false)
        .compact()
        .init();

    let cli = cli::Cli::parse_args();

    match cli.command {
        cli::Command::Export(args) => export::run(args).await,
        cli::Command::Import(args) => import::run(args).await,
        cli::Command::Bench(args)  => bench::run(args).await,
    }
}
