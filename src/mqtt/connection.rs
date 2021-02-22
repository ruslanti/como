use std::borrow::{Borrow, BorrowMut};
use std::net::SocketAddr;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Error, Result};
use futures::{SinkExt, StreamExt, TryFutureExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{debug, field, instrument, trace};
use uuid::Uuid;

use crate::mqtt::context::AppContext;
use crate::mqtt::proto::types::{Auth, Connect, ControlPacket, Disconnect, MQTTCodec, ReasonCode};
use crate::mqtt::session::{Session, SubscriptionEvent};
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
        msg: Connect,
        response_tx: Sender<ControlPacket>,
        subscription_tx: Sender<SubscriptionEvent>,
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

        let mut session = self.context.make_session(
            identifier.clone(),
            response_tx,
            subscription_tx,
            self.peer,
            msg.properties.clone(),
            msg.will.clone(),
        );

        session.handle_msg(ControlPacket::Connect(msg)).await?;

        Ok(session)
    }

    //#[instrument(skip(self, response_tx, subscription_tx), err)]
    async fn recv(
        &mut self,
        msg: ControlPacket,
        response_tx: &Sender<ControlPacket>,
        subscription_tx: &Sender<SubscriptionEvent>,
    ) -> Result<()> {
        //trace!("{:?}", packet);
        match (self.session.borrow_mut(), msg) {
            (None, ControlPacket::Connect(connect)) => {
                self.session = Some(
                    self.prepare_session(connect, response_tx.clone(), subscription_tx.clone())
                        .await?,
                );
                Ok(())
            }
            (None, ControlPacket::Auth(_auth)) => unimplemented!(), //self.process_auth(auth).await,
            (None, packet) => {
                let context = format!("session: None, packet: {:?}", packet,);
                Err(anyhow!("unacceptable event").context(context))
            }
            (Some(_), ControlPacket::PingReq) => response_tx
                .send(ControlPacket::PingResp)
                .map_err(Error::msg)
                .await
                .context("Failure on sending PING resp"),
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

    /*    async fn process_stream<S>(
        &mut self,
        mut stream: S,
        response_tx: Sender<ControlPacket>,
        subscription_tx: Sender<SubscriptionEvent>,
    ) -> Result<()>
    where
        S: Stream<Item = Result<ControlPacket, anyhow::Error>> + Unpin,
    {
        loop {
            let duration = self.keep_alive.add(Duration::from_millis(100));

            match timeout(duration, stream.next()).await? {
                Some(res) => {
                    self.recv(res?, response_tx.borrow(), subscription_tx.borrow())
                        .await?;
                }
                None => {
                    debug!("None request. Disconnected");
                    break;
                }
            }
        }
        Ok(())
    }*/
    /*
    #[instrument(skip(self, subscriber_rx), fields(remote = field::display(& self.peer)), err)]
    async fn process_subscriptions(
        &mut self,
        mut subscriber_rx: Receiver<SubscriptionEvent>,
    ) -> Result<()> {
        while let Some(event) = subscriber_rx.recv().await {
            self.subscription_event(event).await?
        }
        Ok(())
    }*/

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
                        self.recv(p, response_tx.borrow(), subscription_tx.borrow()).await?;
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
