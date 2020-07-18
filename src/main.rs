mod mqtt;

use std::io;
use tokio::signal;
use anyhow::{anyhow, Result};
use mqtt::service;
use tracing::Level;
use tokio::net::TcpListener;

#[macro_use]
extern crate anyhow;

#[tokio::main]
async fn main() -> Result<()> {
    // a builder for `FmtSubscriber`.
    let subscriber = tracing_subscriber::fmt().with_max_level(Level::TRACE).finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("no global subscriber has been set");

    // Bind a TCP listener
    let port = 1883;
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;
    service::run(listener, signal::ctrl_c()).await
}