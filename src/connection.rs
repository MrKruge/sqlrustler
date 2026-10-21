use anyhow::{bail, Context, Result};
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use std::sync::Arc;
use tiberius::{AuthMethod, Config, EncryptionLevel};
use tokio_util::compat::Compat;

use crate::cli::ConnectArgs;

pub type DbPool = Arc<Pool<ConnectionManager>>;
/// Concrete tiberius client type vended by bb8-tiberius with tokio
pub type TiberiusClient = tiberius::Client<Compat<tokio::net::TcpStream>>;

pub async fn build_pool(args: &ConnectArgs, pool_size: u32) -> Result<DbPool> {
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
    config.packet_size(32767);

    // Bound command timeout so tiberius can detect server silence and return a clean
    // error. Without this, a stalled TCP connection hangs forever at the tiberius layer
    // regardless of any tokio::time::timeout wrappers above it.
    config.command_timeout(Some(std::time::Duration::from_secs(120)));

    // Azure SQL always requires encryption
    config.encryption(EncryptionLevel::Required);

    if args.trust_cert {
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

    let manager = ConnectionManager::build(config)
        .context("Failed to build tiberius ConnectionManager")?;

    let pool = Pool::builder()
        .max_size(pool_size)
        .connection_timeout(std::time::Duration::from_secs(60))
        .build(manager)
        .await
        .context("Failed to build connection pool")?;

    Ok(Arc::new(pool))
}

async fn fetch_az_token() -> Result<String> {
    tracing::info!("Fetching Azure AD token via `az account get-access-token`...");
    let output = tokio::process::Command::new("az")
        .args(["account", "get-access-token",
               "--resource", "https://database.windows.net/",
               "--query", "accessToken", "--output", "tsv"])
        .output()
        .await
        .context("Failed to run `az account get-access-token` — is Azure CLI installed and logged in?")?;

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
