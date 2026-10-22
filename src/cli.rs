use clap::{Args, Parser, Subcommand};
use std::fmt;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name    = "sqlrustler",
    version = env!("CARGO_PKG_VERSION"),
    about   = "Fast Azure SQL Database export/import (open-source BACPAC replacement)",
    long_about = "Fast Azure SQL Database export/import — open-source BACPAC replacement.\n\
                  \n\
                  EXAMPLES:\n\
                  \n\
                  # Check connectivity, bandwidth and local write speed before exporting\n\
                  sqlrustler test -H myserver.database.windows.net -d mydb --azure-auth\n\
                  \n\
                  # Test only the network bandwidth layer\n\
                  sqlrustler test -H myserver.database.windows.net -d mydb --azure-auth --only bandwidth\n\
                  \n\
                  # Export with maximum throughput\n\
                  sqlrustler export -H myserver.database.windows.net -d mydb --azure-auth --pressure full -o mydb.sqlrustler\n\
                  \n\
                  # Export with SQL auth\n\
                  sqlrustler export -H myserver.database.windows.net -d mydb -u myuser -p mypassword -o mydb.sqlrustler\n\
                  \n\
                  # Import into a target database\n\
                  sqlrustler import -H myserver.database.windows.net -d mydb_restore --azure-auth -i mydb.sqlrustler\n\
                  \n\
                  # Benchmark sqlrustler vs sqlpackage side-by-side\n\
                  sqlrustler bench -H myserver.database.windows.net -d mydb --azure-auth --compare sqlpackage\n\
                  \n\
                  # Deep concurrency ramp — find the Azure SQL connection ceiling\n\
                  sqlrustler pressure -H myserver.database.windows.net -d mydb --azure-auth --max-connections 32",
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
    /// Pressure-test each layer: local Parquet sink → network pipe → Azure SQL
    Pressure(PressureArgs),
    /// Quick connectivity and throughput check: source DB, bandwidth, and local write speed
    Test(TestArgs),
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

    /// Use current `az cli` session for Azure AD auth (runs `az account get-access-token` internally)
    #[arg(long, default_value = "false")]
    pub azure_auth: bool,

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
            .field("azure_auth", &self.azure_auth)
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

    /// Rows per RecordBatch fetch (larger = fewer round-trips, more RAM)
    #[arg(long, default_value = "10000")]
    pub batch_size: usize,

    /// Pressure preset — overrides --parallel and --batch-size:
    ///   light  = 4 parallel,  10k batch  (easy on DB)
    ///   medium = 8 parallel,  50k batch  (balanced)
    ///   full   = 16 parallel, 50k batch  (maximum throughput)
    #[arg(long, value_parser = ["light", "medium", "full"])]
    pub pressure: Option<String>,

    /// Comma-separated glob patterns for tables to exclude (e.g. "tmp_*,log_*")
    #[arg(long, value_delimiter = ',')]
    pub exclude_tables: Vec<String>,

    /// Export schema DDL only — no table data
    #[arg(long, default_value = "false")]
    pub schema_only: bool,

    /// zstd compression level (1=fastest, 22=best; default 3)
    #[arg(long, default_value = "3")]
    pub compression_level: i32,

    /// After export, verify row counts in archive match live DB counts
    #[arg(long, default_value = "false")]
    pub validate: bool,

    /// Parallel connections per large table (splits by PK range).
    /// Each large table gets this many concurrent connections; the global --parallel limit
    /// still caps total concurrent workers. Default 4 = good balance; set to 1 to disable.
    #[arg(long, default_value = "8")]
    pub table_parallelism: usize,

    /// Row count threshold above which --table-parallelism kicks in.
    #[arg(long, default_value = "500000")]
    pub table_parallelism_threshold: u64,

    /// Enable live monitoring agents: samples local CPU/RAM, network bandwidth,
    /// and Azure SQL metrics (DTU/CPU/connections) during the export.
    /// Prints a timeline report when the export completes.
    #[arg(long, default_value = "false")]
    pub monitor: bool,

    /// Azure resource group for the SQL server (used by --monitor to pull live Azure metrics).
    /// If omitted, sqlrustler will attempt to discover it via `az sql server list`.
    #[arg(long)]
    pub azure_resource_group: Option<String>,
}

impl ExportArgs {
    /// Resolve effective (parallel, batch_size) after applying --pressure preset.
    pub fn effective_parallelism(&self) -> (usize, usize) {
        match self.pressure.as_deref() {
            Some("light")  => (4,  10_000),
            Some("medium") => (8,  50_000),
            Some("full")   => (8, 100_000),
            _              => (self.parallel, self.batch_size),
        }
    }

    /// Effective intra-table parallelism — preset overrides explicit flag.
    pub fn effective_table_parallelism(&self) -> usize {
        match self.pressure.as_deref() {
            Some("full")   => self.table_parallelism.max(4),
            _              => self.table_parallelism,
        }    }
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

#[derive(Args, Debug)]
pub struct PressureArgs {
    #[command(flatten)]
    pub conn: ConnectArgs,

    /// Table to use for network and DB ramp phases (defaults to largest table found)
    #[arg(long)]
    pub table: Option<String>,

    /// Comma-separated columns to select (or "all"); default is the first 5 columns
    #[arg(long)]
    pub columns: Option<String>,

    /// Stop the network probe after this many rows (TOP n)
    #[arg(long)]
    pub max_rows: Option<u64>,

    /// Restrict the network probe to LO:HI on the table's chunk key (e.g. 4019374:4203370)
    #[arg(long)]
    pub id_range: Option<String>,

    /// Max concurrent connections to try during the DB ramp phase
    #[arg(long, default_value = "32")]
    pub max_connections: usize,

    /// Rows per fetch batch during network/DB phases
    #[arg(long, default_value = "100000")]
    pub batch_size: usize,

    /// Seconds to sustain each concurrency level before stepping up
    #[arg(long, default_value = "10")]
    pub step_secs: u64,

    /// Skip the local Parquet sink phase (DB connection required for all phases)
    #[arg(long, default_value = "false")]
    pub skip_local: bool,

    /// Skip the network probe phase
    #[arg(long, default_value = "false")]
    pub skip_network: bool,

    /// Skip the DB ramp phase
    #[arg(long, default_value = "false")]
    pub skip_db_ramp: bool,

    /// Azure resource group for the SQL server (enables live Azure SQL metrics
    /// — cpu_percent, workers_percent, connections — during the DB ramp phase).
    /// If omitted, sqlrustler will attempt to discover it via `az sql server list`.
    #[arg(long)]
    pub azure_resource_group: Option<String>,
}

#[derive(Args, Debug)]
pub struct TestArgs {
    #[command(flatten)]
    pub conn: ConnectArgs,

    /// Which layer to test: source, bandwidth, destination, or all (default: all)
    ///
    ///   source      — connect to the DB, measure query latency and row throughput
    ///   bandwidth   — stream rows over the wire, measure MB/s and rows/s
    ///   destination — write synthetic Parquet locally, measure encode/compress speed
    ///   all         — run all three in order
    #[arg(long, value_parser = ["source", "bandwidth", "destination", "all"], default_value = "all")]
    pub only: String,

    /// Rows to stream for the bandwidth test
    #[arg(long, default_value = "500000")]
    pub bandwidth_rows: u64,

    /// Rows to write for the destination (local Parquet) test
    #[arg(long, default_value = "1000000")]
    pub destination_rows: usize,
}

impl TestArgs {
    pub fn test_source(&self)      -> bool { matches!(self.only.as_str(), "source"      | "all") }
    pub fn test_bandwidth(&self)   -> bool { matches!(self.only.as_str(), "bandwidth"   | "all") }
    pub fn test_destination(&self) -> bool { matches!(self.only.as_str(), "destination" | "all") }
}

