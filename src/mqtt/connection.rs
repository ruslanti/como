use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::ops::{Add, Div};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::{BufMut, Bytes, BytesMut};
use futures::future;
use futures::{Sink, SinkExt, Stream, StreamExt, TryFutureExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::pin;
use tokio::sync::broadcast::RecvError::Lagged;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock, Semaphore};
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{debug, error, instrument, trace, warn};
use uuid::Uuid;

use crate::mqtt::connection::ExactlyOnceState::Received;
use crate::mqtt::context::{AppContext, SessionContext};
use crate::mqtt::proto::property::{
    ConnAckProperties, ConnectProperties, DisconnectProperties, PubResProperties,
    PublishProperties, SubAckProperties,
};
use crate::mqtt::proto::types::{
    Auth, ConnAck, Connect, ControlPacket, Disconnect, MQTTCodec, PacketType, PubResp, Publish,
    QoS, ReasonCode, SubAck, Subscribe, UnSubscribe, Will,
};
use crate::mqtt::session::{Session, SessionEvent};
use crate::mqtt::shutdown::Shutdown;
use crate::mqtt::topic::{Message, Topic};
use crate::settings::{ConnectionSettings, Settings};

//use tokio::stream::StreamExt;

#[derive(Debug)]
enum ExactlyOnceState {
    Init,
    Received,
    Released,
}

type ExactlyOnceReceiver = mpsc::Receiver<(ControlPacket, oneshot::Sender<ControlPacket>)>;
type ExactlyOnceSender = mpsc::Sender<(ControlPacket, oneshot::Sender<ControlPacket>)>;

#[derive(Debug)]
struct ExactlyOnceFlow {
    id: u16,
    rx: ExactlyOnceReceiver,
    state: ExactlyOnceState,
}

#[derive(Debug)]
pub struct ConnectionHandler {
    limit_connections: Arc<Semaphore>,
    sessions_states_tx: mpsc::Sender<ControlPacket>,
    shutdown_complete: mpsc::Sender<()>,
    keep_alive: Option<Duration>,
    publish_flows: BTreeMap<u16, ExactlyOnceSender>,
    context: Arc<Mutex<AppContext>>,
    session: SessionContext,
    config: ConnectionSettings,
}

impl ConnectionHandler {
    pub(crate) fn new(
        limit_connections: Arc<Semaphore>,
        sessions_states_tx: mpsc::Sender<ControlPacket>,
        shutdown_complete: mpsc::Sender<()>,
        context: Arc<Mutex<AppContext>>,
        config: ConnectionSettings,
    ) -> Self {
        ConnectionHandler {
            limit_connections,
            sessions_states_tx,
            shutdown_complete,
            keep_alive: Some(Duration::from_millis(config.idle_keep_alive as u64)),
            publish_flows: Default::default(),
            context,
            session: None,
            config,
        }
    }

    async fn process_connect(
        &mut self,
        msg: Connect,
        mut conn_tx: Sender<ControlPacket>,
    ) -> Result<()> {
        let (assigned_client_identifier, identifier) = if let Some(id) = msg.client_identifier {
            (None, id)
        } else {
            let id: Bytes = Uuid::new_v4().to_hyphenated().to_string().into();
            (Some(id.clone()), id)
        };

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
        let id = std::str::from_utf8(&identifier[..])?;
        let mut context = self.context.lock().await;
        let session_tx = context.connect_session(id, conn_tx).await;
        self.session = Some((
            id.to_string(),
            session_tx,
            self.config.session_expire_interval,
        ));
        Ok(())
    }

    /*

        async fn process_subscribe(&self, subscribe: Subscribe) -> Result<()> {
            debug!("subscribe topics: {:?}", subscribe.topic_filters);

            let root = self.topic_manager.read().await;
            let reason_codes = subscribe.topic_filters.iter().map(|(topic_name, topic_option)| {
                match std::str::from_utf8(&topic_name[..]) {
                    Ok(topic) => {
                        let channels = root.subscribe(topic);
                        for mut channel in channels {
                            tokio::spawn(async move {
                                debug!("new subscribe spawn start  {:?}", channel);
                                loop {
                                    match channel.recv().await {
                                        Ok(msg) => {
                                            debug!("received: {:?}", msg);
                                            let properties = PublishProperties{
                                                payload_format_indicator: None,
                                                message_expire_interval: None,
                                                topic_alias: None,
                                                response_topic: None,
                                                correlation_data: None,
                                                user_properties: vec![],
                                                subscription_identifier: None,
                                                content_type: None
                                            };
                                            let publish = ControlPacket::Publish(Publish{
                                                dup: false,
                                                qos: QoS::AtMostOnce,
                                                retain: false,
                                                topic_name: "".to_string().into(),
                                                packet_identifier: None,
                                                properties,
                                                payload: Default::default()
                                            });
                                            //self.stream.send(publish);
                                        },
                                        Err(Lagged(lag)) => {
                                            warn!("lagged: {}", lag);
                                        }
                                        Err(err) => {
                                            error!(cause = ?err, "topic error: ");
                                            break
                                        }
                                    }
                                }
                                debug!("new subscribe spawn stop {:?}", channel);
                            });
                        }
                        ReasonCode::Success
                    },
                    Err(err) => {
                        debug!(cause = ?err, "subscribe error: ");
                        ReasonCode::TopicFilterInvalid
                    }
                }
            }).collect();
            let suback = ControlPacket::SubAck(SubAck{
                packet_identifier: subscribe.packet_identifier,
                properties: SubAckProperties { reason_string: None, user_properties: vec![] },
                reason_codes
            });
            trace!("send {:?}", suback);
            //self.stream.send(suback).await?;
            Ok(())
        }

        async fn process_unsubscribe(&self, unsubscribe: UnSubscribe) -> Result<()> {
            debug!("unsubscribe topics: {:?}", unsubscribe.topic_filters);
            let reason_codes = unsubscribe.topic_filters.iter().map(|_t| ReasonCode::Success).collect();
            let suback = ControlPacket::UnSubAck(SubAck{
                packet_identifier: unsubscribe.packet_identifier,
                properties: SubAckProperties { reason_string: None, user_properties: vec![] },
                reason_codes
            });
            trace!("send {:?}", suback);
            //self.stream.send(suback).await?;
            Ok(())
        }
    */
    async fn process_packet(
        &mut self,
        packet: ControlPacket,
        mut conn_tx: Sender<ControlPacket>,
    ) -> Result<()> {
        trace!("recv {:?}", packet);
        match (self.session.clone(), packet) {
            (None, ControlPacket::Connect(connect)) => self.process_connect(connect, conn_tx).await,
            (None, ControlPacket::Auth(auth)) => unimplemented!(), //self.process_auth(auth).await,
            (None, packet) => {
                error!(
                    "unacceptable packet {:?} in not defined session state",
                    packet
                );
                Err(anyhow!("unacceptable event").context(""))
            }
            (Some((_, mut session_tx, _)), ControlPacket::Publish(publish)) => {
                session_tx
                    .send(SessionEvent::Publish(publish))
                    .map_err(|e| anyhow!("session_tx send error"))
                    .await
            }
            (Some((_, mut session_tx, _)), ControlPacket::PubRel(pubresp)) => {
                session_tx
                    .send(SessionEvent::PubRel(pubresp))
                    .map_err(|e| anyhow!("session_tx send error"))
                    .await
            }
            (Some((_, mut session_tx, _)), ControlPacket::Subscribe(sub)) => {
                session_tx
                    .send(SessionEvent::Subscribe(sub))
                    .map_err(|e| anyhow!("session_tx send error"))
                    .await
            }
            (Some((_, mut session_tx, _)), ControlPacket::UnSubscribe(sub)) => {
                session_tx
                    .send(SessionEvent::UnSubscribe(sub))
                    .map_err(|e| anyhow!("session_tx send error"))
                    .await
            }
            (Some(_), ControlPacket::PingReq) => {
                conn_tx
                    .send(ControlPacket::PingResp)
                    .map_err(|e| anyhow!("conn_tx send error"))
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
                    .map_err(|e| anyhow!("session_tx send error"))
                    .await
            }
            (Some(_), _) => unimplemented!(),
        }
    }

    async fn process_auth(&self, _msg: Auth) -> Result<()> {
        Ok(())
    }

    #[instrument(skip(self))]
    async fn disconnect(&self, reason_code: ReasonCode) -> Result<()> {
        let disc = ControlPacket::Disconnect(Disconnect {
            reason_code,
            properties: Default::default(),
        });
        trace!("send {:?}", disc);
        //self.stream.send(disc).await?;
        Ok(())
    }

    //#[instrument(skip(self))]
    /*    async fn process_next(&mut self) -> Result<(bool)> {
        trace!("process next start");
        if let Some(packet) = stream.next().await {
            match packet {
                Ok(packet) => {
                    //self.process_packet(packet).await?;
                }
                Err(e) => {
                    error!("error on process connection: {}", e);
                    return Err(e.into());
                }
            };
            trace!("process next end");
            Ok((true))
        } else {
            trace!("process next disconnected");
            Ok((false))
        }
    }*/

    //#[instrument(skip(self))]
    async fn process_stream<S>(
        &mut self,
        mut stream: S,
        conn_tx: Sender<ControlPacket>,
    ) -> Result<()>
    where
        S: Stream<Item = Result<ControlPacket, anyhow::Error>> + Unpin,
    {
        trace!("process start");
        loop {
            trace!("stream timeout  {:?}", self.keep_alive);
            let duration = self
                .keep_alive
                .map_or(Duration::from_secs(u16::max_value() as u64), |d| {
                    d.add(d.div(2))
                });
            match timeout(duration, stream.next()).await {
                Ok(timeout_res) => {
                    if let Some(res) = timeout_res {
                        self.process_packet(res?, conn_tx.clone()).await?;
                    } else {
                        trace!("process next disconnected");
                        break;
                    }
                }
                Err(e) => {
                    error!("timeout on process connection: {}", e);
                    // handle timeout
                    match self.session.as_mut() {
                        None => return Err(e.into()),
                        Some((id, session_tx, expire)) => {
                            {
                                let mut context = self.context.lock().await;
                                context.disconnect_session(&id, *expire).await;
                            }
                            session_tx
                                .send(SessionEvent::Disconnect(Disconnect {
                                    reason_code: ReasonCode::KeepAliveTimeout,
                                    properties: Default::default(),
                                }))
                                .await?;
                            self.disconnect(ReasonCode::KeepAliveTimeout).await?;
                            break;
                        }
                    }
                }
            }
        }
        trace!("process end");
        Ok(())
    }

    //#[instrument(skip(self))]
    pub(crate) async fn run<S>(&mut self, socket: S, mut shutdown: Shutdown) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        trace!("run start");
        let framed = Framed::new(socket, MQTTCodec::new());
        let (sink, stream) = framed.split::<ControlPacket>();
        let (conn_tx, connection_rx) = mpsc::channel(32);
        while !shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown signal.
            tokio::select! {
                res = self.process_stream(stream, conn_tx) => {
                    res?; // handle error
                    break; // disconnect
                },
                res = process_sink(sink, connection_rx) => {
                    res?; // handle error
                    break; // disconnect
                },
                _ = shutdown.recv() => {
                    self.disconnect(ReasonCode::ServerShuttingDown).await?;
                    break;
                }
            };
        }
        trace!("run end");
        Ok(())
    }
}

//#[instrument(skip(self))]
async fn process_sink<S>(mut sink: S, mut reply: Receiver<ControlPacket>) -> Result<()>
where
    S: Sink<ControlPacket> + Unpin,
{
    trace!("process sink start");
    while let Some(msg) = reply.next().await {
        debug!("reply: {:?}", msg);
        sink.send(msg)
            .map_err(|e| anyhow!("sink send error"))
            .await?;
    }
    /*    pin!(sink);
    reply.map(|msg| {
        debug!("reply: {:?}", msg);
        Ok(msg)
    }
    ).forward(&mut sink).await;*/
    trace!("process sink end");
    Ok(())
}

impl Drop for ConnectionHandler {
    fn drop(&mut self) {
        self.limit_connections.add_permits(1);
    }
}
