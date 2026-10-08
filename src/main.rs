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
        cli::Command::Tui          => tui::run_tui().await,
    }
}
