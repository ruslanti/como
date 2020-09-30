use std::net::SocketAddr;
use std::ops::{Add, Div};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Error, Result};
use bytes::Bytes;
use futures::{Sink, SinkExt, Stream, StreamExt, TryFutureExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{debug, error, field, instrument, trace};
use uuid::Uuid;

use crate::mqtt::context::{AppContext, SessionContext};
use crate::mqtt::proto::property::ConnAckProperties;
use crate::mqtt::proto::types::{
    Auth, ConnAck, Connect, ControlPacket, Disconnect, MQTTCodec, QoS, ReasonCode,
};
use crate::mqtt::session::SessionEvent;
use crate::mqtt::shutdown::Shutdown;
use crate::settings::ConnectionSettings;

#[derive(Debug)]
pub struct ConnectionHandler {
    peer: SocketAddr,
    limit_connections: Arc<Semaphore>,
    shutdown_complete: mpsc::Sender<()>,
    keep_alive: Option<Duration>,
    context: Arc<Mutex<AppContext>>,
    session: SessionContext,
    config: ConnectionSettings,
}

impl ConnectionHandler {
    pub(crate) fn new(
        peer: SocketAddr,
        limit_connections: Arc<Semaphore>,
        shutdown_complete: mpsc::Sender<()>,
        context: Arc<Mutex<AppContext>>,
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

    #[instrument(skip(self, conn_tx, msg), err)]
    async fn connect(&mut self, msg: Connect, mut conn_tx: Sender<ControlPacket>) -> Result<()> {
        trace!("{:?}", msg);
        let (assigned_client_identifier, identifier) = if let Some(id) = msg.client_identifier {
            (None, id)
        } else {
            let id: Bytes = Uuid::new_v4().to_hyphenated().to_string().into();
            (Some(id.clone()), id)
        };

        let id = std::str::from_utf8(&identifier[..])?;
        debug!("client identifier: {}", id);

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

        let maximum_qos = if let Some(QoS::ExactlyOnce) = self.config.maximum_qos {
            None
        } else {
            self.config.maximum_qos
        };
        let ack = ControlPacket::ConnAck(ConnAck {
            session_present: false,
            reason_code: ReasonCode::Success,
            properties: ConnAckProperties {
                session_expire_interval: self.config.session_expire_interval,
                receive_maximum: self.config.receive_maximum,
                maximum_qos,
                retain_available: self.config.retain_available,
                maximum_packet_size: self.config.maximum_packet_size,
                assigned_client_identifier,
                topic_alias_maximum: self.config.topic_alias_maximum,
                reason_string: None,
                user_properties: vec![],
                wildcard_subscription_available: None,
                subscription_identifier_available: None,
                shared_subscription_available: None,
                server_keep_alive: self.config.server_keep_alive,
                response_information: None,
                server_reference: None,
                authentication_method: None,
                authentication_data: None,
            },
        });

        conn_tx.send(ack).await?;
        let mut context = self.context.lock().await;
        let session_tx = context.connect_session(id, conn_tx).await;
        self.session = Some((
            id.to_string(),
            session_tx,
            self.config.session_expire_interval,
        ));
        Ok(())
    }

    #[instrument(skip(self, packet, conn_tx), err)]
    async fn receiving(
        &mut self,
        packet: ControlPacket,
        mut conn_tx: Sender<ControlPacket>,
    ) -> Result<()> {
        //trace!("{:?}", packet);
        match (self.session.clone(), packet) {
            (None, ControlPacket::Connect(connect)) => self.connect(connect, conn_tx).await,
            (None, ControlPacket::Auth(_auth)) => unimplemented!(), //self.process_auth(auth).await,
            (None, packet) => {
                let context = format!("session: None, packet: {:?}", packet, );
                Err(anyhow!("unacceptable event").context(context))
            }
            (Some((_, mut session_tx, _)), ControlPacket::Publish(publish)) => {
                session_tx
                    .send(SessionEvent::Publish(publish))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, mut session_tx, _)), ControlPacket::PubAck(response)) => {
                session_tx
                    .send(SessionEvent::PubAck(response))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, mut session_tx, _)), ControlPacket::PubRel(response)) => {
                session_tx
                    .send(SessionEvent::PubRel(response))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, mut session_tx, _)), ControlPacket::PubRec(response)) => {
                session_tx
                    .send(SessionEvent::PubRec(response))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, mut session_tx, _)), ControlPacket::PubComp(response)) => {
                session_tx
                    .send(SessionEvent::PubComp(response))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, mut session_tx, _)), ControlPacket::Subscribe(sub)) => {
                session_tx
                    .send(SessionEvent::Subscribe(sub))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, mut session_tx, _)), ControlPacket::SubAck(sub)) => {
                session_tx
                    .send(SessionEvent::SubAck(sub))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, mut session_tx, _)), ControlPacket::UnSubscribe(sub)) => {
                session_tx
                    .send(SessionEvent::UnSubscribe(sub))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, mut session_tx, _)), ControlPacket::UnSubAck(sub)) => {
                session_tx
                    .send(SessionEvent::UnSubAck(sub))
                    .map_err(Error::msg)
                    .await
            }
            (Some(_), ControlPacket::PingReq) => {
                conn_tx
                    .send(ControlPacket::PingResp)
                    .map_err(Error::msg)
                    .await
            }
            (Some((id, mut session_tx, expire)), ControlPacket::Disconnect(disconnect)) => {
                debug!(
                    "disconnect reason code: {:?}, reason string:{:?}",
                    disconnect.reason_code, disconnect.properties.reason_string
                );
                self.session = None;
                {
                    let mut context = self.context.lock().await;
                    context.disconnect_session(&id, expire).await;
                }
                session_tx
                    .send(SessionEvent::Disconnect(disconnect))
                    .map_err(Error::msg)
                    .await
            }
            (Some((id, _, _)), packet) => {
                let context = format!("session: {}, packet: {:?}", id, packet, );
                Err(anyhow!("unacceptable event").context(context))
            },
        }
    }

    async fn _process_auth(&self, msg: Auth) -> Result<()> {
        trace!("{:?}", msg);
        Ok(())
    }

    #[instrument(skip(self, conn_tx), err)]
    async fn disconnect(
        &self,
        mut conn_tx: Sender<ControlPacket>,
        reason_code: ReasonCode,
    ) -> Result<()> {
        let disc = ControlPacket::Disconnect(Disconnect {
            reason_code,
            properties: Default::default(),
        });
        conn_tx.send(disc).await.map_err(Error::msg)
    }

    async fn process_stream<S>(
        &mut self,
        mut stream: S,
        conn_tx: Sender<ControlPacket>,
    ) -> Result<()>
    where
        S: Stream<Item = Result<ControlPacket, anyhow::Error>> + Unpin,
    {
        loop {
            let duration = self
                .keep_alive
                .map_or(Duration::from_secs(u16::max_value() as u64), |d| {
                    d.add(d.div(2))
                });
            match timeout(duration, stream.next()).await {
                Ok(timeout_res) => {
                    if let Some(res) = timeout_res {
                        self.receiving(res?, conn_tx.clone()).await?;
                    } else {
                        trace!("disconnected");
                        break;
                    }
                }
                Err(e) => {
                    error!("timeout on process connection: {}", e);
                    // handle timeout
                    match self.session.as_mut() {
                        None => return Err(e.into()),
                        Some((id, _session_tx, expire)) => {
                            {
                                let mut context = self.context.lock().await;
                                context.disconnect_session(&id, *expire).await;
                            }
                            self.disconnect(conn_tx, ReasonCode::KeepAliveTimeout)
                                .await?;
                            break;
                        }
                    }
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
        let (conn_tx, conn_rx) = mpsc::channel(32);
        while !shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown signal.
            tokio::select! {
                res = self.process_stream(stream, conn_tx) => {
                    res?; // handle error
                    break; // disconnect
                },
                res = sending(sink, conn_rx) => {
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
async fn sending<S>(mut sink: S, mut reply: Receiver<ControlPacket>) -> Result<()>
where
    S: Sink<ControlPacket> + Unpin,
{
    while let Some(msg) = reply.next().await {
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
