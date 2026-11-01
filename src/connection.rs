use anyhow::{bail, Context, Result};
use bb8::Pool;
use std::sync::Arc;
use tiberius::{AuthMethod, Config, EncryptionLevel};
use tokio_util::compat::Compat;

use crate::cli::ConnectArgs;
use crate::pool::ConnectionManager;

pub type DbPool = Arc<Pool<ConnectionManager>>;
/// Concrete tiberius client type vended by bb8-tiberius with tokio
pub type TiberiusClient = tiberius::Client<Compat<tokio::net::TcpStream>>;

pub async fn build_pool(args: &ConnectArgs, pool_size: u32) -> Result<DbPool> {
    // Policy check first: fail before any token is fetched or any network I/O happens.
    check_tls_policy(
        &args.host,
        args.trust_cert,
        args.azure_auth || args.aad_token.is_some(),
    )?;

    // Resolve AAD token — either passed directly, via env, or fetched from az cli
    let resolved_token: Option<String> = if args.azure_auth {
        Some(fetch_az_token().await?)
    } else {
        args.aad_token.clone()
    };

    let mut config = Config::new();

    config.host(&args.host);
    config.port(args.port);
    config.database(&args.database);

    // Max TDS packet size — critical for nvarchar(max)/large LOB columns.
    // Default 4096 bytes causes excessive re-framing on wide rows; 32767 is the TDS maximum.
    // tiberius 0.13 exposes packet_size() directly.
    let packet_size = parse_packet_size(std::env::var("SQLRUSTLER_PACKET_SIZE").ok().as_deref());
    config.packet_size(packet_size);

    // Bound command timeout so tiberius can detect server silence and return a clean
    // error. Without this, a stalled TCP connection hangs forever at the tiberius layer
    // regardless of any tokio::time::timeout wrappers above it.
    config.command_timeout(Some(std::time::Duration::from_secs(120)));

    // Azure SQL always requires encryption
    config.encryption(EncryptionLevel::Required);

    // Refuse to disable certificate validation where it is never needed (Azure SQL
    // always presents a valid cert) or where a bearer token would be exposed to a MITM.
    if args.trust_cert {
        tracing::warn!("--trust-cert: server certificate validation is DISABLED");
        config.trust_cert();
    }

    match (&resolved_token, &args.user, &args.password) {
        (Some(token), _, _) => {
            config.authentication(AuthMethod::aad_token(token));
        }
        (None, Some(user), Some(pass)) => {
            config.authentication(AuthMethod::sql_server(user, pass));
        }
        (None, Some(user), None) => {
            let pass = prompt_password(user)?;
            config.authentication(AuthMethod::sql_server(user, &pass));
        }
        (None, None, _) => {
            bail!(
                "Provide --azure-auth, --aad-token / SQLRUSTLER_AAD_TOKEN, \
                 or --user + --password for authentication"
            );
        }
    }

    let manager = ConnectionManager::new(config);

    let pool = Pool::builder()
        .max_size(pool_size)
        .connection_timeout(std::time::Duration::from_secs(60))
        .build(manager)
        .await
        .context("Failed to build connection pool")?;

    Ok(Arc::new(pool))
}

/// Parse the optional `SQLRUSTLER_PACKET_SIZE` override; out-of-range or invalid
/// values fall back to the TDS maximum (32767).
fn parse_packet_size(raw: Option<&str>) -> u32 {
    raw.and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|&n| (512..=32767).contains(&n))
        .unwrap_or(32767)
}

/// Azure SQL / Synapse endpoint suffixes (all clouds). These always present a CA-signed cert.
const AZURE_SQL_SUFFIXES: &[&str] = &[
    ".database.windows.net",
    ".database.chinacloudapi.cn",
    ".database.usgovcloudapi.net",
    ".database.cloudapi.de",
    ".sql.azuresynapse.net",
];

fn is_azure_sql_host(host: &str) -> bool {
    let h = host.trim().to_ascii_lowercase();
    let h = h.strip_prefix("tcp:").unwrap_or(&h);
    // Drop any ",port" / ":port" / "\\instance" suffix so it cannot be used to dodge the suffix check.
    let h = h.split([',', ':', '\\', '/']).next().unwrap_or("");
    let h = h.trim_end_matches('.');
    AZURE_SQL_SUFFIXES.iter().any(|s| h.ends_with(s))
}

/// `--trust-cert` turns off TLS server verification. Disallow it for Azure SQL hosts
/// (never needed) and whenever an AAD bearer token is sent (a MITM could replay it
/// against any Azure SQL server).
fn check_tls_policy(host: &str, trust_cert: bool, uses_aad_token: bool) -> Result<()> {
    if !trust_cert {
        return Ok(());
    }
    if is_azure_sql_host(host) {
        bail!("--trust-cert is refused for Azure SQL host {host:?}: Azure SQL always presents a valid certificate");
    }
    if uses_aad_token {
        bail!("--trust-cert is refused with Azure AD token authentication: it would expose the bearer token to a man-in-the-middle");
    }
    Ok(())
}

async fn fetch_az_token() -> Result<String> {
    tracing::info!("Fetching Azure AD token via `az account get-access-token`...");
    let output = tokio::process::Command::new("az")
        .args([
            "account",
            "get-access-token",
            "--resource",
            "https://database.windows.net/",
            "--query",
            "accessToken",
            "--output",
            "tsv",
        ])
        .output()
        .await
        .context(
            "Failed to run `az account get-access-token` — is Azure CLI installed and logged in?",
        )?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("az account get-access-token failed: {stderr}");
    }

    let token = String::from_utf8(output.stdout)
        .context("az output was not valid UTF-8")?
        .trim()
        .to_string();

    if token.is_empty() {
        bail!("az account get-access-token returned an empty token — try `az login` first");
    }

    tracing::info!("Azure AD token acquired ({} chars)", token.len());
    Ok(token)
}

fn prompt_password(user: &str) -> Result<String> {
    eprint!("Password for {user}: ");
    rpassword::read_password().context("Failed to read password from terminal")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_size_default_and_bounds() {
        assert_eq!(parse_packet_size(None), 32767);
        assert_eq!(parse_packet_size(Some("4096")), 4096);
        assert_eq!(parse_packet_size(Some(" 512 ")), 512);
        assert_eq!(parse_packet_size(Some("511")), 32767);
        assert_eq!(parse_packet_size(Some("32768")), 32767);
        assert_eq!(parse_packet_size(Some("-1")), 32767);
        assert_eq!(parse_packet_size(Some("abc")), 32767);
    }

    #[test]
    fn azure_host_detection() {
        assert!(is_azure_sql_host("srv.database.windows.net"));
        assert!(is_azure_sql_host("SRV.Database.Windows.NET."));
        assert!(is_azure_sql_host("tcp:srv.database.windows.net"));
        assert!(is_azure_sql_host("srv.database.usgovcloudapi.net"));
        assert!(is_azure_sql_host("srv.database.windows.net,1433"));
        assert!(is_azure_sql_host(" srv.database.windows.net:1433 "));
        assert!(!is_azure_sql_host("evil.com,srv.database.windows.net"));
        assert!(!is_azure_sql_host("localhost"));
        assert!(!is_azure_sql_host("evil.example.com"));
        assert!(!is_azure_sql_host("database.windows.net.evil.com"));
        assert!(!is_azure_sql_host("xdatabase.windows.net"));
    }

    #[test]
    fn trust_cert_refused_for_azure_host() {
        assert!(check_tls_policy("srv.database.windows.net", true, false).is_err());
    }

    #[test]
    fn trust_cert_refused_with_aad_token() {
        assert!(check_tls_policy("10.0.0.5", true, true).is_err());
    }

    #[test]
    fn trust_cert_allowed_for_local_sql_login() {
        assert!(check_tls_policy("localhost", true, false).is_ok());
    }

    #[test]
    fn no_trust_cert_always_ok() {
        assert!(check_tls_policy("srv.database.windows.net", false, true).is_ok());
    }

    fn sample_args() -> ConnectArgs {
        ConnectArgs {
            host: "h".into(),
            database: "d".into(),
            user: Some("u".into()),
            password: Some("hunter2-SECRET".into()),
            aad_token: Some("eyJ.TOKEN-SECRET".into()),
            azure_auth: false,
            port: 1433,
            trust_cert: false,
        }
    }

    #[test]
    fn connect_args_debug_redacts_secrets() {
        let dbg = format!("{:?}", sample_args());
        assert!(!dbg.contains("hunter2-SECRET"), "{dbg}");
        assert!(!dbg.contains("TOKEN-SECRET"), "{dbg}");
    }

    #[test]
    fn tiberius_config_debug_redacts_secrets() {
        let mut c = Config::new();
        c.authentication(AuthMethod::sql_server("u", "hunter2-SECRET"));
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("hunter2-SECRET"), "{dbg}");
        let mut c = Config::new();
        c.authentication(AuthMethod::aad_token("TOKEN-SECRET"));
        assert!(!format!("{c:?}").contains("TOKEN-SECRET"));
    }
}
