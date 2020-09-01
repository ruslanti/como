use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore};
use tokio::time::{self, Duration};
use tokio_native_tls::TlsAcceptor;
use tokio_util::codec::Framed;
use tracing::{debug, error, info};

use crate::mqtt::connection::ConnectionHandler;
use crate::mqtt::proto::types::{ControlPacket, MQTTCodec};
use crate::mqtt::shutdown::Shutdown;
use crate::mqtt::topic::Topic;
use crate::settings::ConnectionSettings;

#[derive(Debug)]
struct Service {
    listener: TcpListener,
    acceptor: Option<Arc<TlsAcceptor>>,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    sessions_states_tx: mpsc::Sender<ControlPacket>,
    config: ConnectionSettings,
    topic_manager: Arc<RwLock<Topic>>
}

pub async fn run(listener: TcpListener, acceptor: Option<Arc<TlsAcceptor>>,
                 config: ConnectionSettings, max_connections: usize, shutdown: impl Future,
                    topic_manager: Arc<RwLock<Topic>>) -> Result<()> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
    let (sessions_states_tx, sessions_states_rx) = mpsc::channel(32);

    // Initialize the listener state
    let mut service = Service {
        listener,
        acceptor,
        limit_connections: Arc::new(Semaphore::new(max_connections)),
        notify_shutdown,
        shutdown_complete_rx,
        shutdown_complete_tx,
        sessions_states_tx,
        config,
        topic_manager
    };

    tokio::select! {
        res = service.listen() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            // The shutdown signal has been received.
            info!("shutting down");
        }
    }

    let Service {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = service;

    drop(notify_shutdown); // notify with shutdown
    drop(shutdown_complete_tx); // notify shutdown complete

    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}

impl Service {
    async fn listen(&mut self) -> Result<()> {
        info!("{} accepting inbound connections", self.listener.local_addr().unwrap());
        loop {
            self.limit_connections.acquire().await.forget();

            let stream = self.accept().await?;

            if let Some(acceptor) = self.acceptor.as_ref() {
                let stream = acceptor.accept(stream).await?;
                let mut handler = ConnectionHandler::new(
                    Framed::new(stream, MQTTCodec::new()),
                    self.limit_connections.clone(),
                    self.sessions_states_tx.clone(),
                    self.shutdown_complete_tx.clone(),
                    self.config, self.topic_manager.clone()
                );

                let shutdown = Shutdown::new(self.notify_shutdown.subscribe());
                tokio::spawn(async move {
                    if let Err(err) = handler.run(shutdown).await {
                        error!(cause = ?err, "connection error");
                    }
                });
            } else {
                let mut handler = ConnectionHandler::new(
                    Framed::new(stream, MQTTCodec::new()),
                    self.limit_connections.clone(),
                    self.sessions_states_tx.clone(),
                    self.shutdown_complete_tx.clone(),
                    self.config, self.topic_manager.clone()
                );

                let shutdown = Shutdown::new(self.notify_shutdown.subscribe());
                tokio::spawn(async move {
                    if let Err(err) = handler.run(shutdown).await {
                        error!(cause = ?err, "connection error");
                    }
                });
            }
        }
    }

    async fn accept(&mut self) -> Result<TcpStream> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, address)) => {
                    debug!("inbound connection: {:?} ", address);
                    return Ok(socket)
                },
                Err(err) => {
                    if backoff > 64 {
                        error!("error on accepting connection: {}", err);
                        return Err(err.into());
                    }
                }
            }
            time::delay_for(Duration::from_secs(backoff)).await;
            // Double the back off
            backoff *= 2;
        }
    }
}