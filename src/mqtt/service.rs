use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use anyhow::Result;
use crate::mqtt::connection::ConnectionHandler;
use crate::mqtt::sessions::SessionManager;
use crate::mqtt::shutdown::Shutdown;
use tracing::{trace, debug, error, info, instrument};
use crate::settings::{Settings, ConnectionSettings};
use tokio_util::codec::Framed;
use tokio::stream::StreamExt;
use futures::sink::SinkExt;
use crate::mqtt::proto::types::{MQTTCodec, ControlPacket};

#[derive(Debug)]
struct Service {
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    sessions_states_tx: mpsc::Sender<(ControlPacket)>,
    settings: Settings
}

pub async fn run(listener: TcpListener, settings: Settings, shutdown: impl Future) -> Result<()> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
    let (sessions_states_tx, sessions_states_rx) = mpsc::channel(32);

    // Initialize the listener state
    let mut service = Service {
        listener,
        limit_connections: Arc::new(Semaphore::new(settings.service.max_connections)),
        notify_shutdown,
        shutdown_complete_rx,
        shutdown_complete_tx,
        sessions_states_tx,
        settings
    };

    tokio::select! {
        res = service.listen() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
/*        res = service.sessions_manager(sessions_states_rx) => {
            if let Err(err) = res {
                error!(cause = %err, "failure in sessions manager");
            }
        }*/
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

/*    async fn sessions_manager(&mut self, session_states_rx: mpsc::Receiver<(ControlPacket)>) -> Result<()> {
        let mut sessions = SessionManager::new(
            self.settings.connection, session_states_rx);
        let shutdown_service = Shutdown::new(self.notify_shutdown.subscribe());
        tokio::spawn(async move {
            if let Err(err) = sessions.run(shutdown_service).await {
                error!(cause = ?err, "session manager error");
            }
        });

        Ok(())
    }*/

    async fn listen(&mut self) -> Result<()> {
        info!("accepting inbound connections");
        loop {
            self.limit_connections.acquire().await.forget();

            let socket = self.accept().await?;

            let mut handler = ConnectionHandler::new(
                Framed::new(socket, MQTTCodec::new()),
                self.limit_connections.clone(),
                self.sessions_states_tx.clone(),
                self.shutdown_complete_tx.clone(),
                self.settings.connection,
            );

            let shutdown = Shutdown::new(self.notify_shutdown.subscribe());
            tokio::spawn(async move {
                if let Err(err) = handler.run(shutdown).await {
                    error!(cause = ?err, "connection error");
                }
            });
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