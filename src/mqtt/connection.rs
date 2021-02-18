use std::borrow::BorrowMut;
use std::net::SocketAddr;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Error, Result};
use futures::{Sink, SinkExt, Stream, StreamExt, TryFutureExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{error, field, instrument, trace};
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
    keep_alive: Option<Duration>,
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
            keep_alive: Some(Duration::from_millis(config.idle_keep_alive as u64)),
            context,
            session: None,
            config,
        }
    }

    //#[instrument(skip(self, conn_tx, msg), err)]
    async fn prepare_session(
        &mut self,
        msg: Connect,
        conn_tx: Sender<ControlPacket>,
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
            .map(|d| Duration::from_secs(d as u64));

        let mut session = self.context.make_session(
            identifier.clone(),
            conn_tx,
            self.peer,
            msg.properties.clone(),
            msg.will.clone(),
        );

        session.handle(ControlPacket::Connect(msg)).await?;

        Ok(session)
    }

    #[instrument(skip(self, conn_tx), err)]
    async fn recv(&mut self, msg: ControlPacket, conn_tx: Sender<ControlPacket>) -> Result<()> {
        //trace!("{:?}", packet);
        match (self.session.borrow_mut(), msg) {
            (None, ControlPacket::Connect(connect)) => {
                self.session = Some(self.prepare_session(connect, conn_tx).await?);
                Ok(())
            }
            (None, ControlPacket::Auth(_auth)) => unimplemented!(), //self.process_auth(auth).await,
            (None, packet) => {
                let context = format!("session: None, packet: {:?}", packet,);
                Err(anyhow!("unacceptable event").context(context))
            }
            (Some(_), ControlPacket::PingReq) => conn_tx
                .send(ControlPacket::PingResp)
                .map_err(Error::msg)
                .await
                .context("Failure on sending PING resp"),
            (Some(session), msg) => session.handle(msg).await,
        }
    }

    /*    #[instrument(skip(self), err)]
    async fn event(&self, event: SessionEvent) -> Result<()> {
        match &self.session {
            Some((_, tx)) => tx.send(event).map_err(Error::msg).await,
            None => Ok(()),
        }
    }*/

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

    async fn process_stream<S>(&mut self, mut stream: S, tx: Sender<ControlPacket>) -> Result<()>
    where
        S: Stream<Item = Result<ControlPacket, anyhow::Error>> + Unpin,
    {
        loop {
            let duration = self
                .keep_alive
                .map_or(Duration::from_secs(u16::max_value() as u64), |d| {
                    d.add(Duration::from_millis(100))
                });
            match timeout(duration, stream.next()).await {
                Ok(timeout_res) => {
                    if let Some(res) = timeout_res {
                        self.recv(res?, tx.clone()).await?;
                    } else {
                        trace!("disconnected");
                        break;
                    }
                }
                Err(e) => {
                    error!("timeout on process connection: {}", e);
                    // handle timeout
                    /*match self.session.as_mut() {
                        None => return Err(e.into()),
                        Some((id, _session_tx)) => {
                            trace!("DISCONNECT {}", id);
                            self.event(SessionEvent::Disconnect(Disconnect {
                                reason_code: ReasonCode::KeepAliveTimeout,
                                properties: Default::default(),
                            }))
                            .await?;
                            self.disconnect(tx, ReasonCode::KeepAliveTimeout).await?;
                            break;
                        }
                    }*/
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self, socket, shutdown), fields(remote = field::display(& self.peer)), err)]
    pub(crate) async fn connection<S>(&mut self, socket: S, mut shutdown: Shutdown) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let framed = Framed::new(socket, MQTTCodec::new());
        let (sink, stream) = framed.split::<ControlPacket>();
        let (session_tx, session_rx) = mpsc::channel(32);
        while !shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown signal.
            tokio::select! {
                res = self.process_stream(stream, session_tx) => {
                    res?; // handle error
                    break; // disconnect
                },
                res = send(sink, session_rx) => {
                    res?; // handle error
                    break; // disconnect
                },
                _ = shutdown.recv() => {
                    //self.disconnect(conn_tx.clone(), ReasonCode::ServerShuttingDown).await?;
                    break;
                }
            };
        }
        Ok(())
    }
}

#[instrument(skip(sink, reply), err)]
async fn send<S>(mut sink: S, mut reply: Receiver<ControlPacket>) -> Result<()>
where
    S: Sink<ControlPacket> + Unpin,
{
    while let Some(msg) = reply.recv().await {
        trace!("{:?}", msg);
        if let Err(_err) = sink.send(msg).await {
            bail!("socket send error");
        }
    }
    Ok(())
}

impl Drop for ConnectionHandler {
    fn drop(&mut self) {
        self.limit_connections.add_permits(1);
    }
}
