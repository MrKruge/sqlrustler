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
    let mut config = Config::new();

    config.host(&args.host);
    config.port(args.port);
    config.database(&args.database);

    // Azure SQL always requires encryption
    config.encryption(EncryptionLevel::Required);

    if args.trust_cert {
        config.trust_cert();
    }

    match (&args.aad_token, &args.user, &args.password) {
        (Some(token), _, _) => {
            // TODO(aad-device-flow): add MSAL device-code flow via azure_identity crate
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
                "Provide either --aad-token / SQLRUSTLER_AAD_TOKEN \
                 or --user + --password for authentication"
            );
        }
    }

    let manager = ConnectionManager::build(config)
        .context("Failed to build tiberius ConnectionManager")?;

    let pool = Pool::builder()
        .max_size(pool_size)
        .build(manager)
        .await
        .context("Failed to build connection pool")?;

    Ok(Arc::new(pool))
}

fn prompt_password(user: &str) -> Result<String> {
    eprint!("Password for {user}: ");
    rpassword::read_password().context("Failed to read password from terminal")
}
