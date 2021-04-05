use std::borrow::Borrow;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, Barrier, Semaphore};
use tokio_rustls::rustls::internal::pemfile::{certs, rsa_private_keys};
use tokio_rustls::rustls::{Certificate, NoClientAuth, PrivateKey, ServerConfig};
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, instrument};

use crate::connection::ConnectionHandler;
use crate::context::SessionContext;
use crate::service::accept;
use crate::shutdown::Shutdown;

/*use native_tls::Identity;
use native_tls::TlsAcceptor;*/
pub(crate) struct TlsTransport {
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    context: SessionContext,
    ready: Arc<Barrier>,
}

fn load_certs(path: &Path) -> io::Result<Vec<Certificate>> {
    certs(&mut BufReader::new(File::open(path)?))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid cert"))
}

fn load_keys(path: &Path) -> io::Result<Vec<PrivateKey>> {
    rsa_private_keys(&mut BufReader::new(File::open(path)?))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid key"))
}

impl TlsTransport {
    pub fn new(
        limit_connections: Arc<Semaphore>,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
        context: SessionContext,
        ready: Arc<Barrier>,
    ) -> Self {
        TlsTransport {
            limit_connections,
            notify_shutdown,
            shutdown_complete_tx,
            context,
            ready,
        }
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn listen(&mut self) -> Result<()> {
        if let Some(tls) = self.context.settings().service.tls.to_owned() {
            let address = format!("{}:{}", tls.bind, tls.port);
            let listener = TcpListener::bind(&address).await?;

            let certs = load_certs(tls.cert.as_ref())?;
            let mut keys = load_keys(tls.pass.as_ref())?;

            let mut config = ServerConfig::new(NoClientAuth::new());
            config
                .set_single_cert(certs, keys.remove(0))
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
            let acceptor = TlsAcceptor::from(Arc::new(config));

            self.ready.wait().await;
            info!("accepting inbound TLS connections: {}", address);

            loop {
                self.limit_connections.acquire().await?.forget();

                let stream = accept(listener.borrow()).await?;

                let mut handler = ConnectionHandler::new(
                    "tls",
                    stream.peer_addr()?,
                    self.limit_connections.clone(),
                    self.shutdown_complete_tx.clone(),
                    self.context.clone(),
                );

                let shutdown = Shutdown::new(self.notify_shutdown.subscribe());

                let stream = acceptor.accept(stream).await?;
                tokio::spawn(async move {
                    if let Err(err) = handler.client(stream, shutdown).await {
                        error!(cause = ?err, "connection error");
                    }
                });
            }
        };
        Ok(())
    }
}
