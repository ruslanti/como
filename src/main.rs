mod settings;
mod mqtt;

use tokio::signal;
use anyhow::Result;
use mqtt::service;
use tracing::Level;
use tokio::net::TcpListener;
use native_tls::Identity;
use native_tls::TlsAcceptor;
use crate::settings::Settings;
use tracing::debug;
use std::fs::File;
use std::sync::Arc;
use std::io::Read;

#[tokio::main]
async fn main() -> Result<()> {
    // a builder for `FmtSubscriber`.
    let subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("no global subscriber has been set");

    let settings = Settings::new()?;
    debug!("{:?}", settings);

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("{}:{}", settings.service.bind, settings.service.port)).await?;
    let srv = service::run(listener, None,settings.connection, settings.service.max_connections, signal::ctrl_c());

    if let Some(tls) = settings.service.tls {
        // Bind a TLS listener
        let mut file = File::open(tls.cert)?;
        let mut identity = vec![];
        file.read_to_end(&mut identity)?;
        let identity = Identity::from_pkcs12(&identity, tls.pass.as_str())?;

        let acceptor = TlsAcceptor::new(identity)?;
        let acceptor = Arc::new(acceptor.into());

        let tls_listener = TcpListener::bind(&format!("{}:{}", tls.bind, tls.port)).await?;
        let tls_srv = service::run(tls_listener, Some(acceptor),settings.connection, settings.service.max_connections, signal::ctrl_c());
        let (res, tls_res) = tokio::join!( srv, tls_srv);
        res.and(tls_res)
    } else {
        srv.await
    }
}