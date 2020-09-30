use std::fs::File;
use std::io::Read;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use native_tls::Identity;
use native_tls::TlsAcceptor;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::Mutex;
use tracing::{debug, Level};

use mqtt::service;

use crate::mqtt::context::AppContext;
use crate::settings::Settings;

mod mqtt;
mod settings;

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Arc::new(Settings::new()?);

    // a builder for `FmtSubscriber`.
    let (non_blocking, _guard) = if let Some(file) = settings.log.file.clone() {
        tracing_appender::non_blocking(tracing_appender::rolling::daily("logs", file))
    } else {
        tracing_appender::non_blocking(std::io::stdout())
    };
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::from_str(settings.log.level.as_str())?)
        .with_ansi(false).with_writer(non_blocking).finish();
    tracing::subscriber::set_global_default(subscriber).expect("no global subscriber has been set");

    debug!("{:?}", settings);

    let context = Arc::new(Mutex::new(AppContext::new(settings.clone())));

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!(
        "{}:{}",
        settings.service.bind, settings.service.port
    ))
    .await?;
    let srv = service::run(
        listener,
        None,
        settings.clone(),
        signal::ctrl_c(),
        context.clone(),
    );

    if let Some(tls) = &settings.clone().service.tls {
        // Bind a TLS listener
        let cert = &tls.cert;
        let mut file = File::open(cert).with_context(|| format!("could not open cert file: {}", cert))?;
        let mut identity = vec![];
        file.read_to_end(&mut identity).with_context(|| format!("could not read cert file: {}", cert))?;
        let identity = Identity::from_pkcs12(&identity, tls.pass.as_str()).with_context(|| format!("could not read identity from cert file: {}", cert))?;

        let acceptor = TlsAcceptor::new(identity).context("TLS acceptor fail")?;
        let acceptor = Arc::new(acceptor.into());

        let tls_listener = TcpListener::bind(&format!("{}:{}", tls.bind, tls.port)).await?;
        let tls_srv = service::run(
            tls_listener,
            Some(acceptor),
            settings.clone(),
            signal::ctrl_c(),
            context.clone(),
        );
        let (res, tls_res) = tokio::join!(srv, tls_srv);
        res.and(tls_res)
    } else {
        srv.await
    }
}
