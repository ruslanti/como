use std::net::SocketAddr;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Error, Result};
use bytes::Bytes;
use futures::{Sink, SinkExt, Stream, StreamExt, TryFutureExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{debug, error, field, instrument, trace};
use uuid::Uuid;

use crate::mqtt::context::AppContext;
use crate::mqtt::proto::property::ConnAckProperties;
use crate::mqtt::proto::types::{
    Auth, ConnAck, Connect, ControlPacket, Disconnect, MQTTCodec, QoS, ReasonCode,
};
use crate::mqtt::session::SessionEvent;
use crate::mqtt::shutdown::Shutdown;
use crate::settings::ConnectionSettings;

type SessionContext = Option<(String, Sender<SessionEvent>)>;

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
    async fn connect(&mut self, msg: Connect, conn_tx: Sender<ControlPacket>) -> Result<()> {
        //trace!("{:?}", msg);
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

        let mut context = self.context.lock().await;
        let (session_event_tx, connection_context_tx) =
            context.connect(id, msg.clean_start_flag).await;

        connection_context_tx
            .send((
                conn_tx.clone(),
                msg.properties.session_expire_interval.unwrap_or(0),
                msg.will,
            ))
            .await?;

        self.session = Some((id.to_string(), session_event_tx));

        conn_tx.send(ack).await?;

        Ok(())
    }

    #[instrument(skip(self, packet, conn_tx), err)]
    async fn recv(&mut self, packet: ControlPacket, conn_tx: Sender<ControlPacket>) -> Result<()> {
        trace!("{:?}", packet);
        match (self.session.clone(), packet) {
            (None, ControlPacket::Connect(connect)) => self.connect(connect, conn_tx).await,
            (None, ControlPacket::Auth(_auth)) => unimplemented!(), //self.process_auth(auth).await,
            (None, packet) => {
                let context = format!("session: None, packet: {:?}", packet,);
                Err(anyhow!("unacceptable event").context(context))
            }
            (Some((_, session_tx)), ControlPacket::Publish(publish)) => {
                session_tx
                    .send(SessionEvent::Publish(publish))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, session_tx)), ControlPacket::PubAck(response)) => {
                session_tx
                    .send(SessionEvent::PubAck(response))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, session_tx)), ControlPacket::PubRel(response)) => {
                session_tx
                    .send(SessionEvent::PubRel(response))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, session_tx)), ControlPacket::PubRec(response)) => {
                session_tx
                    .send(SessionEvent::PubRec(response))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, session_tx)), ControlPacket::PubComp(response)) => {
                session_tx
                    .send(SessionEvent::PubComp(response))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, session_tx)), ControlPacket::Subscribe(sub)) => {
                session_tx
                    .send(SessionEvent::Subscribe(sub))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, session_tx)), ControlPacket::SubAck(sub)) => {
                session_tx
                    .send(SessionEvent::SubAck(sub))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, session_tx)), ControlPacket::UnSubscribe(sub)) => {
                session_tx
                    .send(SessionEvent::UnSubscribe(sub))
                    .map_err(Error::msg)
                    .await
            }
            (Some((_, session_tx)), ControlPacket::UnSubAck(sub)) => {
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
            (Some((id, session_tx)), ControlPacket::Disconnect(disconnect)) => {
                debug!(
                    "{}: disconnect reason code: {:?}, reason string:{:?}",
                    id, disconnect.reason_code, disconnect.properties.reason_string
                );
                self.session = None;
                /*                {
                    let mut context = self.context.lock().await;
                    context.disconnect(&id, expire).await;
                }*/
                session_tx
                    .send(SessionEvent::Disconnect(disconnect))
                    .map_err(Error::msg)
                    .await
            }
            (Some((id, _)), packet) => {
                let context = format!("session: {}, packet: {:?}", id, packet,);
                Err(anyhow!("unacceptable event").context(context))
            }
        }
    }

    #[instrument(skip(self), err)]
    async fn event(&self, event: SessionEvent) -> Result<()> {
        match self.session.clone() {
            Some((_, tx)) => tx.send(event).map_err(Error::msg).await,
            None => Ok(()),
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
                    match self.session.as_mut() {
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
                res = send(sink, conn_rx) => {
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
