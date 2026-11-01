use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

/// bb8 connection manager for tiberius over tokio.
pub struct ConnectionManager {
    config: Config,
}

impl ConnectionManager {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    async fn open(config: &Config) -> Result<Client<Compat<TcpStream>>, tiberius::error::Error> {
        let tcp = TcpStream::connect(config.get_addr()).await?;
        tcp.set_nodelay(true)?;
        Client::connect(config.clone(), tcp.compat_write()).await
    }
}

impl bb8::ManageConnection for ConnectionManager {
    type Connection = Client<Compat<TcpStream>>;
    type Error = tiberius::error::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        match Self::open(&self.config).await {
            // Azure SQL redirect: reconnect once to the address the server names.
            Err(tiberius::error::Error::Routing { host, port }) => {
                let mut config = self.config.clone();
                config.host(&host);
                config.port(port);
                Self::open(&config).await
            }
            other => other,
        }
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.simple_query("SELECT 1").await?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}
