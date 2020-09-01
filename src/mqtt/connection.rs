use std::collections::BTreeMap;
use std::ops::Add;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::{BufMut, Bytes, BytesMut};
use futures::{SinkExt, TryFutureExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::stream::StreamExt;
use tokio::sync::{mpsc, oneshot, RwLock, Semaphore};
use tokio::sync::broadcast::RecvError::Lagged;
use tokio::time::timeout;
use tokio_util::codec::Framed;
use tracing::{debug, error, instrument, trace, warn};
use uuid::Uuid;

use crate::mqtt::connection::ExactlyOnceState::Received;
use crate::mqtt::proto::property::{ConnAckProperties, ConnectProperties, DisconnectProperties, PublishProperties, PubResProperties, SubAckProperties};
use crate::mqtt::proto::types::{Auth, ConnAck, Connect, ControlPacket, Disconnect, MQTTCodec, PacketType, Publish, PubResp, QoS, ReasonCode, SubAck, Subscribe, UnSubscribe, Will};
use crate::mqtt::shutdown::Shutdown;
use crate::mqtt::topic::{Message, Topic};
use crate::settings::ConnectionSettings;

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
    state: ExactlyOnceState
}

#[derive(Debug)]
pub(crate) enum State {
    Idle,
    Auth,
    Established{
        client_identifier: Bytes,
        keep_alive: u16,
        properties: ConnectProperties,
        will: Option<Will>
    },
    Disconnected
}


#[derive(Debug)]
pub struct ConnectionHandler<S> {
    stream: Framed<S, MQTTCodec>,
    limit_connections: Arc<Semaphore>,
    sessions_states_tx: mpsc::Sender<ControlPacket>,
    shutdown_complete: mpsc::Sender<()>,
    config: ConnectionSettings,
    state: State,
    connection_rx: mpsc::Receiver<()>,
    stream_timeout: Option<Duration>,
    publish_flows: BTreeMap<u16, ExactlyOnceSender>,
    topic_manager: Arc<RwLock<Topic>>
}

impl ExactlyOnceFlow {
    fn new(id: u16, rx: ExactlyOnceReceiver) -> Self {
        ExactlyOnceFlow { id, rx, state: ExactlyOnceState::Init }
    }

    async fn run(&mut self) -> Result<()> {
        trace!("ExactlyOnceFlow start: {}", self.id);
        while let Ok(Some(cmd)) = timeout(Duration::from_secs(1),self.rx.recv()).await {
            trace!("ExactlyOnceFlow: {:?}", cmd);
            match (&self.state, cmd) {
                (ExactlyOnceState::Init, (ControlPacket::Publish(_msg), tx)) => {
                    //self.topic_tx.send(Message::new(msg.retain, msg.topic_name, msg.payload)).await?;
                    let rec = ControlPacket::PubRec(PubResp {
                        packet_type: PacketType::PUBREC,
                        packet_identifier: self.id,
                        reason_code: ReasonCode::Success,
                        properties: PubResProperties { reason_string: None, user_properties: vec![] }
                    });
                    trace!("send {:?}", rec);
                    self.state = ExactlyOnceState::Received;
                    if let Err(_) = tx.send(rec) {
                        debug!("the receiver dropped");
                    }
                },
                (ExactlyOnceState::Received, (ControlPacket::PubRel(_msg), tx)) => {
                    let comp = ControlPacket::PubComp(PubResp {
                        packet_type: PacketType::PUBCOMP,
                        packet_identifier: self.id,
                        reason_code: ReasonCode::Success,
                        properties: PubResProperties { reason_string: None, user_properties: vec![] }
                    });
                    trace!("send {:?}", comp);
                    self.state = ExactlyOnceState::Released;
                    if let Err(_) = tx.send(comp) {
                        debug!("the receiver dropped");
                    }
                }
                _ => unreachable!()
            }
        }
        trace!("ExactlyOnceFlow end: {}", self.id);
        Ok(())
    }
}

impl<S> ConnectionHandler<S> where S: AsyncRead + AsyncWrite + Unpin {
    pub(crate) fn new(stream: Framed<S, MQTTCodec>, limit_connections: Arc<Semaphore>,
               sessions_states_tx: mpsc::Sender<ControlPacket>,
               shutdown_complete: mpsc::Sender<()>,
               config: ConnectionSettings, topic_manager: Arc<RwLock<Topic>>) -> Self {
        let (_connection_tx, connection_rx) = mpsc::channel(32);
        ConnectionHandler {
            stream,
            limit_connections,
            sessions_states_tx,
            shutdown_complete,
            config,
            state: State::Idle,
            connection_rx,
            stream_timeout: Some(Duration::from_millis(config.idle_keep_alive as u64)),
            publish_flows: Default::default(),
            topic_manager
        }
    }

    async fn process_connect(&mut self, msg: Connect) -> Result<()> {
        let (assigned_client_identifier, identifier) = if let Some(id) = msg.client_identifier {
            (None, id)
        } else {
            let id: Bytes = Uuid::new_v4().to_hyphenated().to_string().into();
            (Some(id.clone()), id)
        };

        let maximum_qos = if let Some(QoS::ExactlyOnce) = self.config.maximum_qos { None } else { self.config.maximum_qos };
        let ack = ControlPacket::ConnAck(ConnAck{
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
                authentication_data: None
            }
        });

        let client_keep_alive = if msg.keep_alive != 0 { Some(msg.keep_alive) } else { None };
        self.stream_timeout = self.config.server_keep_alive.or(client_keep_alive).map(|d| Duration::from_secs(d as u64));

        trace!("send {:?}", ack);
        self.stream.send(ack).await?;

        self.state = State::Established {
            client_identifier: identifier,
            keep_alive: msg.keep_alive,
            properties: msg.properties,
            will: msg.will
        };

        Ok(())
    }

    async fn process_publish(&mut self, msg: Publish) -> Result<()> {
        match msg.qos {
            QoS::AtMostOnce => {
                //todo store topic
                let mut root = self.topic_manager.write().await;
                let topic_name = std::str::from_utf8(&msg.topic_name[..])?;
                let channel = root.publish(topic_name);
                let message = Message{
                    retain: msg.retain,
                    content_type: msg.properties.content_type,
                    data: msg.payload
                };
                if let Err(err) = channel.send(message) {
                    error!(cause = ?err, "publish send error for topic {}", topic_name);
                }
            },
            QoS::AtLeastOnce => {
                if let Some(packet_identifier) = msg.packet_identifier
                {
                    //todo store topic
                    //self.topic_tx.send(Message::new(msg.retain, msg.topic_name, msg.payload)).await?;
                    let ack = ControlPacket::PubAck(PubResp {
                        packet_type: PacketType::PUBACK,
                        packet_identifier,
                        reason_code: ReasonCode::Success,
                        properties: PubResProperties { reason_string: None, user_properties: vec![] },
                    });
                    trace!("send {:?}", ack);
                    self.stream.send(ack).await?;
                } else {
                    return Err(anyhow!("undefined packet_identifier"))
                }
            }
            QoS::ExactlyOnce => {
                if let Some(packet_identifier) = msg.packet_identifier
                {
                    if self.publish_flows.contains_key(&packet_identifier) && !msg.dup {
                        self.disconnect(ReasonCode::PacketIdentifierInUse).await?;
                    } else {
                        let (mut tx, rx) = mpsc::channel(1);
                        let mut flow = ExactlyOnceFlow::new(packet_identifier, rx);
                        self.publish_flows.insert(packet_identifier, tx.clone());

                        tokio::spawn(async move {
                            if let Err(err) = flow.run().await{
                                debug!(cause = ?err, "connection error");
                            }
                            //self.publish_flows.remove(&packet_identifier)
                        });

                        let (resp_tx, resp_rx) = oneshot::channel();
                        tx.send((ControlPacket::Publish(msg), resp_tx)).await?;

                        if let Ok(res) = resp_rx.await {
                            trace!("send {:?}", res);
                            self.stream.send(res).await?;
                            //todo store topic
                        } else {
                            debug!("no response in channel");
                        }
                    }
                } else {
                    return Err(anyhow!("undefined packet_identifier"))
                }
            },
        }
        Ok(())
    }

    async fn process_pubrel(&mut self, msg: PubResp) -> Result<()> {
        if let Some(tx) = self.publish_flows.get(&msg.packet_identifier) {
            let (resp_tx, resp_rx) = oneshot::channel();
            tx.clone().send((ControlPacket::PubRel(msg), resp_tx)).await?;

            if let Ok(res) = resp_rx.await {
                trace!("send {:?}", res);
                self.stream.send(res).await?;
                //todo store topic
            } else {
                debug!("no response in channel");
            }
        } else {
            self.disconnect(ReasonCode::PacketIdentifierNotFound).await?;
        }
        Ok(())
    }

    async fn process_subscribe(&mut self, subscribe: Subscribe) -> Result<()> {
        debug!("subscribe topics: {:?}", subscribe.topic_filters);

        let root = self.topic_manager.write().await;
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
        self.stream.send(suback).await?;
        Ok(())
    }

    async fn process_unsubscribe(&mut self, unsubscribe: UnSubscribe) -> Result<()> {
        debug!("unsubscribe topics: {:?}", unsubscribe.topic_filters);
        let reason_codes = unsubscribe.topic_filters.iter().map(|_t| ReasonCode::Success).collect();
        let suback = ControlPacket::UnSubAck(SubAck{
            packet_identifier: unsubscribe.packet_identifier,
            properties: SubAckProperties { reason_string: None, user_properties: vec![] },
            reason_codes
        });
        trace!("send {:?}", suback);
        self.stream.send(suback).await?;
        Ok(())
    }

    async fn process_packet(&mut self, packet: ControlPacket) -> Result<()> {
        trace!("recv {:?}", packet);
        match (&self.state, packet) {
            (State::Idle, ControlPacket::Connect(connect)) => self.process_connect(connect).await,
            (State::Idle, packet) => {
                error!("unacceptable packet {:?} in idle state", packet);
                Err(anyhow!("unacceptable event").context(""))
            },
            (State::Auth, ControlPacket::Auth(auth)) => self.process_auth(auth).await,
            (State::Auth, packet) => {
                error!("expected Auth, found {:?} in auth state", packet);
                Err(anyhow!("unacceptable event").context(""))
            },
            (State::Established{..}, ControlPacket::Publish(publish)) => self.process_publish(publish).await,
            (State::Established{..}, ControlPacket::PubRel(pubrel)) => self.process_pubrel(pubrel).await,
            (State::Established{..}, ControlPacket::Disconnect(disconnect)) => {
                debug!("disconnect reason code: {:?}, reason string:{:?}", disconnect.reason_code, disconnect.properties.reason_string);
                self.state = State::Disconnected;
                //self.stream.close().await?;
                Ok(())
            },
            (State::Established{..}, ControlPacket::PingReq) => {
                debug!("ping req/res");
                self.stream.send(ControlPacket::PingResp).await?;
                Ok(())
            },
            (State::Established{..}, ControlPacket::Subscribe(subscribe)) => self.process_subscribe(subscribe).await,
            (State::Established{..}, ControlPacket::UnSubscribe(unsubscribe)) => self.process_unsubscribe(unsubscribe).await,
            (State::Established{..}, packet) => {
                error!("unacceptable packet {:?} in established state", packet);
                Err(anyhow!("unacceptable event").context(""))
            },
            _ => unreachable!()
        }
    }

    async fn process_auth(&mut self, _msg: Auth) -> Result<()> {
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn process_stream(&mut self) -> Result<()> {
        trace!("process start");
        loop {
            trace!("stream timeout  {:?}", self.stream_timeout);
            match self.stream_timeout {
                Some(duration) => {
                    match timeout(duration.add(Duration::from_millis(100)), self.process_next()).await {
                        Ok(res) => {
                            if !res? {
                                break
                            }
                        },
                        Err(e) =>  {
                            error!("timeout on process connection: {}", e);
                            // handle timeout
                            match self.state {
                                State::Idle => return Err(e.into()),
                                State::Established{..} => {
                                    self.disconnect(ReasonCode::KeepAliveTimeout).await?;
                                    break;
                                },
                                _ => unreachable!()
                            }

                        }
                    }
                },
                None => {
                    if !self.process_next().await? {
                        break
                    }
                }
            }
        }
        trace!("process end");
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn process_next(&mut self) -> Result<bool> {
        trace!("process next start");
        if let Some(packet) = self.stream.next().await {
            match packet {
                Ok(packet) => {
                    self.process_packet(packet).await?;
                }
                Err(e) => {
                    error!("error on process connection: {}", e);
                    return Err(e.into());
                }
            }
            trace!("process next end");
            Ok(true)
        } else {
            trace!("process next disconnected");
            Ok(false)
        }
    }

    #[instrument(skip(self))]
    pub async fn disconnect(&mut self, reason_code: ReasonCode) -> Result<()> {
        debug!("send disconnect");

        let disc = ControlPacket::Disconnect(Disconnect{
            reason_code,
            properties: DisconnectProperties {
                session_expire_interval: None,
                reason_string: None,
                user_properties: vec![],
                server_reference: None
            }
        });
        trace!("send {:?}", disc);
        self.stream.send(disc).await?;
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn process_sink(&self) -> Result<()> {
        trace!("process sink start");
        loop {}
        trace!("process sink end");
        Ok(())
    }

    //#[instrument(skip(self))]
    pub(crate) async fn run(&mut self, mut shutdown: Shutdown) -> Result<()> {
        trace!("run start");
        while !shutdown.is_shutdown() {  
            // While reading a request frame, also listen for the shutdown signal.
            tokio::select! {
                res = self.process_stream() => {
                    res?; // handle error
                    break; // disconnect
                },
/*                res = self.process_sink() => {
                    res?; // handle error
                    break; // disconnect
                },*/
                _ = shutdown.recv() => {
                    trace!("shutdown received");
                    self.disconnect(ReasonCode::ServerShuttingDown).await?;
                    trace!("disconnect sent");
                    return Ok(());
                }
            }
            ;
        }
        trace!("run end");
        Ok(())
    }
}

impl<S> Drop for ConnectionHandler<S> {
    fn drop(&mut self) {
        self.limit_connections.add_permits(1);
    }
}
