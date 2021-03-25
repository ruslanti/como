use std::borrow::Borrow;
use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Barrier, Semaphore};
use tokio::time::{sleep, Duration};
use tracing::{error, info, instrument, warn};

use crate::connection::ConnectionHandler;
use crate::context::SessionContext;
use crate::settings::Settings;
use crate::shutdown::Shutdown;
use crate::tls_service::TlsTransport;

#[derive(Debug)]
pub struct Service {
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

struct TcpTransport {
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    context: SessionContext,
    ready: Arc<Barrier>,
}

pub async fn run(settings: Arc<Settings>, shutdown: impl Future) -> Result<()> {
    let ready = Arc::new(Barrier::new(1));
    run_with_ready(settings, shutdown, ready).await
}

pub async fn run_with_ready(
    settings: Arc<Settings>,
    shutdown: impl Future,
    ready: Arc<Barrier>,
) -> Result<()> {
    let limit_connections = Arc::new(Semaphore::new(settings.service.max_connections));

    let context = SessionContext::new(settings)?;

    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    // Bind a TCP listener
    let mut tcp_transport = TcpTransport::new(
        limit_connections.clone(),
        notify_shutdown.clone(),
        shutdown_complete_tx.clone(),
        context.clone(),
        ready.clone(),
    );

    let use_tls = context.settings().service.tls.is_some();
    let mut tls_transport = TlsTransport::new(
        limit_connections.clone(),
        notify_shutdown.clone(),
        shutdown_complete_tx.clone(),
        context.clone(),
        ready.clone(),
    );

    // Initialize the listener state
    let service = Service {
        notify_shutdown,
        shutdown_complete_rx,
        shutdown_complete_tx,
    };

    tokio::select! {
        res = tcp_transport.listen() => {
            if let Err(err) = res {
                error!(cause = ?err, "failed to accept");
            }
        }
        res = tls_transport.listen(), if use_tls => {
            if let Err(err) = res {
                error!(cause = ?err, "failed to accept tls");
            }
        }
        _ = shutdown => {
            // The shutdown signal has been received.
            info!("shutting down signal");
        }
    }

    let Service {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = service;

    drop(tcp_transport);
    drop(tls_transport);

    drop(notify_shutdown); // notify with shutdown
    drop(shutdown_complete_tx); // notify shutdown complete

    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}

#[instrument(skip(listener), err)]
pub(crate) async fn accept(listener: &TcpListener) -> Result<TcpStream> {
    let mut backoff = 1;

    loop {
        match listener.accept().await {
            Ok((socket, address)) => {
                info!("inbound connection: {:?} ", address);
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
        // Double the back off
        backoff *= 2;
    }
}

impl TcpTransport {
    pub fn new(
        limit_connections: Arc<Semaphore>,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
        context: SessionContext,
        ready: Arc<Barrier>,
    ) -> Self {
        TcpTransport {
            limit_connections,
            notify_shutdown,
            shutdown_complete_tx,
            context,
            ready,
        }
    }

    #[instrument(skip(self), err)]
    async fn listen(&mut self) -> Result<()> {
        let transport = self.context.settings().service.borrow();
        let address = format!("{}:{}", transport.bind, transport.port);
        let listener = TcpListener::bind(&address).await?;
        self.ready.wait().await;
        info!("accepting inbound connections: {}", address);

        loop {
            self.limit_connections.acquire().await?.forget();

            let stream = accept(listener.borrow()).await?;

            let mut handler = ConnectionHandler::new(
                "tcp",
                stream.peer_addr()?,
                self.limit_connections.clone(),
                self.shutdown_complete_tx.clone(),
                self.context.clone(),
            );

            let shutdown = Shutdown::new(self.notify_shutdown.subscribe());

            tokio::spawn(async move {
                if let Err(err) = handler.client(stream, shutdown).await {
                    warn!(cause = ?err, "connection {} error", handler.peer);
                } else {
                    info!("connection {} closed", handler.peer)
                }
            });
        }
    }
}
