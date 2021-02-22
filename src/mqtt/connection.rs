use std::borrow::{Borrow, BorrowMut};
use std::net::SocketAddr;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Error, Result};
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{debug, field, instrument, trace};
use uuid::Uuid;

use crate::mqtt::context::AppContext;
use crate::mqtt::proto::types::{Auth, Connect, ControlPacket, Disconnect, MQTTCodec, ReasonCode};
use crate::mqtt::session::Session;
use crate::mqtt::shutdown::Shutdown;
use crate::settings::ConnectionSettings;

#[derive(Debug)]
pub struct ConnectionHandler {
    peer: SocketAddr,
    limit_connections: Arc<Semaphore>,
    shutdown_complete: mpsc::Sender<()>,
    keep_alive: Duration,
    context: Arc<AppContext>,
    session: Option<Session>,
    config: ConnectionSettings,
}

impl ConnectionHandler {
    pub(crate) fn new(
        peer: SocketAddr,
        limit_connections: Arc<Semaphore>,
        shutdown_complete: mpsc::Sender<()>,
        context: Arc<AppContext>,
        config: ConnectionSettings,
    ) -> Self {
        ConnectionHandler {
            peer,
            limit_connections,
            shutdown_complete,
            keep_alive: Duration::from_millis(config.idle_keep_alive as u64),
            context,
            session: None,
            config,
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
            .unwrap_or(Uuid::new_v4().to_simple().to_string().into());

        //let id = std::str::from_utf8(&identifier[..])?;
        //debug!("client identifier: {}", id);

        let client_keep_alive = if msg.keep_alive != 0 {
            Some(msg.keep_alive)
        } else {
            None
        };
        self.keep_alive = self
            .config
            .server_keep_alive
            .or(client_keep_alive)
            .map(|d| Duration::from_secs(d as u64))
            .unwrap_or(self.keep_alive);
        trace!("keep_alive: {:?}", self.keep_alive);

        let session = self.context.make_session(
            identifier.clone(),
            response_tx,
            self.peer,
            msg.properties.clone(),
            msg.will.clone(),
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
                let context = format!("session: None, packet: {:?}", packet,);
                Err(anyhow!("unacceptable event").context(context))
            }
            (Some(_), ControlPacket::PingReq) => Ok(Some(ControlPacket::PingResp)),
            (Some(session), msg) => session.handle_msg(msg).await,
        }
    }

    async fn session(&mut self) -> Result<()> {
        match self.session.borrow_mut() {
            None => Err(anyhow!("unacceptable event").context("subscription event")),
            Some(session) => session.session().await,
        }
    }

    async fn _process_auth(&self, msg: Auth) -> Result<()> {
        trace!("{:?}", msg);
        Ok(())
    }

    #[instrument(skip(self, conn_tx), err)]
    async fn disconnect(
        &self,
        conn_tx: Sender<ControlPacket>,
        reason_code: ReasonCode,
    ) -> Result<()> {
        let disc = ControlPacket::Disconnect(Disconnect {
            reason_code,
            properties: Default::default(),
        });
        conn_tx.send(disc).await.map_err(Error::msg)
    }

    #[instrument(skip(self, socket, shutdown), fields(peer = field::display(& self.peer)), err)]
    pub(crate) async fn client<S>(&mut self, socket: S, mut shutdown: Shutdown) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let framed = Framed::new(socket, MQTTCodec::new());
        let (mut sink, mut stream) = framed.split::<ControlPacket>();
        let (response_tx, mut response_rx) = mpsc::channel::<ControlPacket>(32);

        while !shutdown.is_shutdown() {
            let duration = self.keep_alive.add(Duration::from_millis(100));
            // While reading a request frame, also listen for the shutdown signal.
            trace!("select with timeout {} ms", duration.as_millis());
            tokio::select! {
                res = timeout(duration, stream.next()) => {
                    if let Some(res) = res? {
                        let p = res?;
                        debug!("received {:?}", p);
                        if let Some(msg) = self.recv(p, response_tx.borrow()).await? {
                            debug!("sending {:?}", msg);
                            sink.send(msg).await.context("socket send error")?
                        }
                    } else {
                        debug!("Disconnected");
                        break;
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
                     res?;
                }
                 _ = shutdown.recv() => {
                     //self.disconnect(conn_tx.clone(), ReasonCode::ServerShuttingDown).await?;
                     break;
                 }
            }
        }
        Ok(())
    }
}
/*
#[instrument(skip(sink, response_rx), err)]
async fn send<S>(mut sink: S, mut response_rx: Receiver<ControlPacket>) -> Result<()>
where
    S: Sink<ControlPacket> + Unpin,
{
    while let Some(msg) = response_rx.recv().await {
        trace!("{:?}", msg);
        if let Err(_err) = sink.send(msg).await {
            bail!("socket send error");
        }
    }
    Ok(())
}*/

impl Drop for ConnectionHandler {
    fn drop(&mut self) {
        self.limit_connections.add_permits(1);
    }
}
