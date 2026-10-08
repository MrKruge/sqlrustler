use clap::{Args, Parser, Subcommand};
use std::fmt;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name    = "sqlrustler",
    version = env!("CARGO_PKG_VERSION"),
    about   = "Fast Azure SQL Database export/import (open-source BACPAC replacement)",
    long_about = None,
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

impl Cli {
    pub fn parse_args() -> Self {
        <Self as clap::Parser>::parse()
    }
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Export a database to a .sqlrustler archive
    Export(ExportArgs),
    /// Import a .sqlrustler archive into a database
    Import(ImportArgs),
    /// Benchmark export speed, optionally comparing against sqlpackage
    Bench(BenchArgs),
    /// Launch the interactive cowboy TUI
    Tui,
}

/// Shared connection parameters — embedded into each subcommand via #[command(flatten)]
/// NOTE: Debug is implemented manually to redact `password` and `aad_token`.
#[derive(Args, Clone)]
pub struct ConnectArgs {
    /// SQL Server hostname (e.g. myserver.database.windows.net)
    #[arg(short = 'H', long, env = "SQLRUSTLER_HOST")]
    pub host: String,

    /// Database name
    #[arg(short = 'd', long, env = "SQLRUSTLER_DATABASE")]
    pub database: String,

    /// SQL login username (omit for Azure AD token auth)
    #[arg(short = 'u', long, env = "SQLRUSTLER_USER")]
    pub user: Option<String>,

    /// SQL login password
    #[arg(short = 'p', long, env = "SQLRUSTLER_PASSWORD")]
    pub password: Option<String>,

    /// Azure AD bearer token (alternative to user/password)
    #[arg(long, env = "SQLRUSTLER_AAD_TOKEN")]
    pub aad_token: Option<String>,

    /// TCP port (default 1433)
    #[arg(long, default_value = "1433")]
    pub port: u16,

    /// Trust server certificate (for dev/self-signed certs)
    #[arg(long, default_value = "false")]
    pub trust_cert: bool,
}

/// Redacts password and aad_token so they never appear in logs or panic output.
impl fmt::Debug for ConnectArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectArgs")
            .field("host", &self.host)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("aad_token", &self.aad_token.as_ref().map(|_| "<redacted>"))
            .field("port", &self.port)
            .field("trust_cert", &self.trust_cert)
            .finish()
    }
}

#[derive(Args, Debug)]
pub struct ExportArgs {
    #[command(flatten)]
    pub conn: ConnectArgs,

    /// Output archive path (default: <database>_<timestamp>.sqlrustler)
    #[arg(short = 'o', long)]
    pub output: Option<PathBuf>,

    /// Maximum number of tables exported in parallel
    #[arg(short = 'j', long, default_value_t = num_cpus::get())]
    pub parallel: usize,

    /// Rows per RecordBatch (affects Parquet row group size)
    #[arg(long, default_value = "10000")]
    pub batch_size: usize,

    /// Comma-separated glob patterns for tables to exclude (e.g. "tmp_*,log_*")
    #[arg(long, value_delimiter = ',')]
    pub exclude_tables: Vec<String>,

    /// Export schema DDL only — no table data
    #[arg(long, default_value = "false")]
    pub schema_only: bool,

    /// zstd compression level (1=fastest, 22=best; default 3)
    #[arg(long, default_value = "3")]
    pub compression_level: i32,
}

#[derive(Args, Debug)]
pub struct ImportArgs {
    #[command(flatten)]
    pub conn: ConnectArgs,

    /// Input archive path
    #[arg(short = 'i', long)]
    pub input: PathBuf,

    /// Maximum number of tables imported in parallel
    #[arg(short = 'j', long, default_value_t = num_cpus::get())]
    pub parallel: usize,

    /// Rows per INSERT batch
    #[arg(long, default_value = "1000")]
    pub batch_size: usize,

    /// Import data only — skip DDL execution
    #[arg(long, default_value = "false")]
    pub data_only: bool,

    /// Drop and recreate the database before import
    #[arg(long, default_value = "false")]
    pub drop_existing: bool,
}

#[derive(Args, Debug)]
pub struct BenchArgs {
    #[command(flatten)]
    pub conn: ConnectArgs,

    /// Output archive path for the benchmark run
    #[arg(short = 'o', long)]
    pub output: Option<PathBuf>,

    /// Compare against another tool: currently only "sqlpackage" is supported
    #[arg(long, value_parser = ["sqlpackage"])]
    pub compare: Option<String>,
}
