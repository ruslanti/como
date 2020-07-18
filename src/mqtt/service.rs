use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use std::io;
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use anyhow::{anyhow, Result};
use crate::mqtt::session::Session;
use crate::mqtt::shutdown::Shutdown;
use tracing::{trace, debug, error, info, instrument};

const MAX_CONNECTIONS: usize = 250;

#[derive(Debug)]
struct Listener {
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

#[derive(Debug)]
struct Handler {
    session: Session,
    limit_connections: Arc<Semaphore>,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

pub async fn run(listener: TcpListener, shutdown: impl Future) -> Result<()> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    // Initialize the listener state
    let mut service = Listener {
        listener,
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
    };

    tokio::select! {
        res = service.run() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            // The shutdown signal has been received.
            info!("shutting down");
        }
    }

    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = service;

    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}

impl Listener {
    async fn run(&mut self) -> Result<()> {
        info!("accepting inbound connections");
        loop {
            self.limit_connections.acquire().await.forget();

            let socket = self.accept().await?;

            let mut handler = Handler {
                session: Session::new(socket),

                limit_connections: self.limit_connections.clone(),

                // Receive shutdown notifications.
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
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

impl Handler {
   // #[instrument(skip(self))]
    async fn run(&mut self) -> Result<()> {
        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown
            // signal.
            let maybe_frame = tokio::select! {
                res = self.session.process() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };
        }

        Ok(())
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        self.limit_connections.add_permits(1);
    }
}