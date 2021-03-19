use std::borrow::{Borrow, BorrowMut};
use std::convert::TryInto;
use std::net::SocketAddr;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{debug, field, instrument, trace};
use uuid::Uuid;

use como_mqtt::v5::types::{Auth, Connect, ControlPacket, Disconnect, MQTTCodec, ReasonCode};

use crate::context::SessionContext;
use crate::session::Session;
use crate::shutdown::Shutdown;

#[derive(Debug)]
pub struct ConnectionHandler {
    pub(crate) peer: SocketAddr,
    limit_connections: Arc<Semaphore>,
    shutdown_complete: mpsc::Sender<()>,
    keep_alive: Duration,
    context: Arc<SessionContext>,
    session: Option<Session>,
}

impl ConnectionHandler {
    pub(crate) fn new(
        peer: SocketAddr,
        limit_connections: Arc<Semaphore>,
        shutdown_complete: mpsc::Sender<()>,
        context: Arc<SessionContext>,
    ) -> Self {
        ConnectionHandler {
            peer,
            limit_connections,
            shutdown_complete,
            keep_alive: Duration::from_millis(context.settings.connection.idle_keep_alive as u64),
            context,
            session: None,
        }
    }

    //#[instrument(skip(self, conn_tx, msg), err)]
    async fn prepare_session(
        &mut self,
        msg: &Connect,
        response_tx: Sender<ControlPacket>,
    ) -> Result<Session> {
        //trace!("{:?}", msg);
        let identifier = msg
            .client_identifier
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_simple().to_string().into());

        //let id = std::str::from_utf8(&identifier[..])?;
        //debug!("client identifier: {}", id);

        let client_keep_alive = if msg.keep_alive != 0 {
            Some(msg.keep_alive)
        } else {
            None
        };
        self.keep_alive = self
            .context
            .settings
            .connection
            .server_keep_alive
            .or(client_keep_alive)
            .map(|d| Duration::from_secs(d as u64).add(Duration::from_millis(100)))
            .unwrap_or_else(|| Duration::from_micros(u64::MAX));
        trace!("keep_alive: {:?}", self.keep_alive);

        let session = Session::new(
            identifier.try_into()?,
            response_tx,
            self.peer,
            msg.properties.clone(),
            msg.will.clone(),
            self.context.clone(),
        );

        Ok(session)
    }

    //#[instrument(skip(self, response_tx, subscription_tx), err)]
    async fn recv(
        &mut self,
        msg: ControlPacket,
        response_tx: &Sender<ControlPacket>,
    ) -> Result<Option<ControlPacket>> {
        //trace!("{:?}", packet);
        match (self.session.borrow_mut(), msg) {
            (None, ControlPacket::Connect(connect)) => {
                let session = self.prepare_session(&connect, response_tx.clone()).await?;
                self.session
                    .get_or_insert(session)
                    .handle_msg(ControlPacket::Connect(connect))
                    .await
            }
            (None, ControlPacket::Auth(_auth)) => unimplemented!(), //self.process_auth(auth).await,
            (None, packet) => {
                let context = format!("session: None, packet: {}", packet,);
                Err(anyhow!("unacceptable event").context(context))
            }
            (Some(_), ControlPacket::PingReq) => Ok(Some(ControlPacket::PingResp)),
            (Some(session), ControlPacket::Disconnect(disconnect)) => {
                session
                    .handle_msg(ControlPacket::Disconnect(disconnect))
                    .await?;
                self.keep_alive = Duration::from_millis(0); // close socket and session
                Ok(None)
            }
            (Some(session), msg) => session.handle_msg(msg).await,
        }
    }

    async fn session(&mut self) -> Result<Option<ControlPacket>> {
        match self.session.borrow_mut() {
            None => Err(anyhow!("unacceptable event").context("subscription event")),
            Some(session) => session.session().await,
        }
    }

    async fn _process_auth(&self, msg: Auth) -> Result<()> {
        trace!("{:?}", msg);
        Ok(())
    }

    #[instrument(skip(self, socket, shutdown), fields(peer = field::display(& self.peer)))]
    pub async fn client<S>(&mut self, socket: S, mut shutdown: Shutdown) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let framed = Framed::new(socket, MQTTCodec::default());
        let (mut sink, mut stream) = framed.split::<ControlPacket>();
        let (response_tx, mut response_rx) = mpsc::channel::<ControlPacket>(32);
        while !shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown signal.
            //trace!("select with timeout {} ms", duration.as_millis());
            tokio::select! {
                res = timeout(self.keep_alive, stream.next()) => {
                    match res {
                        Ok(res) => {
                            if let Some(res) = res {
                                let p = res?;
                                debug!("received {:?}", p);
                                if let Some(msg) = self.recv(p, response_tx.borrow()).await? {
                                    debug!("sending {:?}", msg);
                                    sink.send(msg).await.context("socket send error")?
                                }
                            } else {
                                debug!("Client disconnected");
                                break;
                            }
                        },
                        Err(_) if self.keep_alive.as_micros() == 0 => break,
                        Err(elapsed) => return Err(anyhow!(elapsed))
                    }
                }
                res = response_rx.recv() => {
                    if let Some(res) = res {
                        debug!("sending {:?}", res);
                        sink.send(res).await.context("socket send error")?
                    } else {
                        debug!("None response. Disconnected");
                        break;
                    }
                }
                res = self.session(), if self.session.is_some() => {
                    trace!("session event: {:?}", res);
                    if let Some(msg) = res? {
                        if let ControlPacket::Disconnect(_) = msg {
                            self.keep_alive = Duration::from_millis(0); // close socket and session
                        }
                        debug!("sending {:?}", msg);
                        sink.send(msg).await.context("socket send error")?;
                    }
                }
                _ = shutdown.recv() => {
                    debug!("server shutting down");
                    if let Some(mut session) = self.session.take() {
                        session.close_immediately().await;
                    }
                    let msg = ControlPacket::Disconnect(Disconnect {reason_code:
                    ReasonCode::ServerShuttingDown, properties: Default::default()});
                    debug!("sending {:?}", msg);
                    sink.send(msg).await.context("socket send error")?;
                    break;
                }
            }
        }
        if let Some(mut session) = self.session.take() {
            tokio::spawn(async move { session.close().await });
        }
        Ok(())
    }
}

impl Drop for ConnectionHandler {
    fn drop(&mut self) {
        self.limit_connections.add_permits(1);
    }
}
