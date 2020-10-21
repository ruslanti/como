use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Mutex, Semaphore};
use tokio::time::{sleep, Duration};
use tokio_native_tls::TlsAcceptor;
use tracing::{debug, error, field, info, instrument};

use crate::mqtt::connection::ConnectionHandler;
use crate::mqtt::context::AppContext;
use crate::mqtt::shutdown::Shutdown;
use crate::settings::Settings;

#[derive(Debug)]
struct Service {
    listener: TcpListener,
    acceptor: Option<Arc<TlsAcceptor>>,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    config: Arc<Settings>,
    context: Arc<Mutex<AppContext>>,
}

pub(crate) async fn run(
    listener: TcpListener,
    acceptor: Option<Arc<TlsAcceptor>>,
    config: Arc<Settings>,
    shutdown: impl Future,
    context: Arc<Mutex<AppContext>>,
) -> Result<()> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    // Initialize the listener state
    let mut service = Service {
        listener,
        acceptor,
        limit_connections: Arc::new(Semaphore::new(config.service.max_connections)),
        notify_shutdown,
        shutdown_complete_rx,
        shutdown_complete_tx,
        config,
        context,
    };

    tokio::select! {
        res = service.listen() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            // The shutdown signal has been received.
            info!("shutting down {}", service.listener.local_addr().unwrap());
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
    #[instrument(skip(self), fields(addr = field::display(& self.listener.local_addr().unwrap())),
    err)]
    async fn listen(&mut self) -> Result<()> {
        info!("accepting inbound connections");
        loop {
            self.limit_connections.acquire().await.forget();

            let stream = self.accept().await?;

            let context = self.context.lock().await;
            let mut handler = ConnectionHandler::new(
                stream.peer_addr()?,
                self.limit_connections.clone(),
                self.shutdown_complete_tx.clone(),
                self.context.clone(),
                context.config.connection,
            );

            let shutdown = Shutdown::new(self.notify_shutdown.subscribe());

            if let Some(acceptor) = self.acceptor.as_ref() {
                let stream = acceptor.accept(stream).await?;
                tokio::spawn(async move {
                    if let Err(err) = handler.connection(stream, shutdown).await {
                        error!(cause = ?err, "connection error");
                    }
                });
            } else {
                tokio::spawn(async move {
                    if let Err(err) = handler.connection(stream, shutdown).await {
                        error!(cause = ?err, "connection error");
                    }
                });
            }
        }
    }

    #[instrument(skip(self), err)]
    async fn accept(&mut self) -> Result<TcpStream> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, address)) => {
                    debug!("inbound connection: {:?} ", address);
                    return Ok(socket);
                }
                Err(err) => {
                    if backoff > 64 {
                        error!("error on accepting connection: {}", err);
                        return Err(err.into());
                    }
                }
            }
            sleep(Duration::from_secs(backoff)).await;
            //time::delay_for(Duration::from_secs(backoff)).await;
            // Double the back off
            backoff *= 2;
        }
    }
}
