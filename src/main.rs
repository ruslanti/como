mod settings;
mod mqtt;

use tokio::signal;
use anyhow::Result;
use mqtt::service;
use tracing::Level;
use tokio::net::TcpListener;
use crate::settings::Settings;
use tracing::debug;

#[tokio::main]
async fn main() -> Result<()> {
    // a builder for `FmtSubscriber`.
    let subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("no global subscriber has been set");

    let settings = Settings::new()?;
    debug!("{:?}", settings);

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("{}:{}", settings.service.listen, settings.service.port)).await?;
    service::run(listener, settings, signal::ctrl_c()).await
}