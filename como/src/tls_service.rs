use std::borrow::Borrow;
use std::io::Read;
use std::sync::Arc;

use anyhow::{Context, Result};
use native_tls::Identity;
use native_tls::TlsAcceptor;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{sleep, Duration};
use tracing::{error, info, instrument};

use crate::connection::ConnectionHandler;
use crate::context::AppContext;
use crate::shutdown::Shutdown;

pub(crate) struct TlsTransport {
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    context: Arc<AppContext>,
}

impl TlsTransport {
    pub fn new(
        limit_connections: Arc<Semaphore>,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
        context: Arc<AppContext>,
    ) -> Self {
        TlsTransport {
            limit_connections,
            notify_shutdown,
            shutdown_complete_tx,
            context,
        }
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn listen(&mut self) -> Result<()> {
        if let Some(tls) = self.context.config.service.tls.to_owned() {
            let address = format!("{}:{}", tls.bind, tls.port);
            info!("accepting inbound TLS connections: {}", address);
            let listener = TcpListener::bind(&address).await?;

            let cert = &tls.cert;
            let mut file =
                std::fs::File::open(cert).context(format!("could not open cert file: {}", cert))?;
            let mut identity = vec![];
            file.read_to_end(&mut identity)
                .context(format!("could not read cert file: {}", cert))?;
            let identity = Identity::from_pkcs12(&identity, tls.pass.as_str())
                .context(format!("could not read identity from cert file: {}", cert))?;

            let acceptor: TlsAcceptor = TlsAcceptor::new(identity).context("TLS acceptor fail")?;
            let acceptor: tokio_native_tls::TlsAcceptor = acceptor.into();

            loop {
                self.limit_connections.acquire().await?.forget();

                let stream = Self::accept(listener.borrow()).await?;

                let mut handler = ConnectionHandler::new(
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

    #[instrument(skip(listener), err)]
    async fn accept(listener: &TcpListener) -> Result<TcpStream> {
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
}
