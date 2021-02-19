use bytes::Bytes;
use std::borrow::Borrow;
use std::cmp::min;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Error, Result};
use byteorder::{BigEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use sled::{Batch, Event, IVec, Subscriber, Tree};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;
use tracing::{debug, error, field, info, instrument, trace, warn};

use crate::mqtt::proto::encoder::EncodedSize;
use crate::mqtt::proto::property::{
    ConnAckProperties, ConnectProperties, PubResProperties, PublishProperties,
};
use crate::mqtt::proto::types::{
    ConnAck, Connect, ControlPacket, Disconnect, MqttString, PacketType, PubResp, Publish, QoS,
    ReasonCode, SubAck, Subscribe, SubscriptionOptions, UnSubscribe, Will,
};
use crate::mqtt::topic::{TopicMessage, Topics};
use crate::settings::ConnectionSettings;

#[derive(Debug, Serialize, Deserialize)]
struct SubscriptionKey<'a> {
    session: &'a [u8],
    topic_filter: &'a [u8],
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SubscriptionEvent {
    id: u64,
    subscription_qos: QoS,
    msg: TopicMessage,
}

impl SubscriptionEvent {
    pub fn new() -> Self {
        SubscriptionEvent {
            id: 0,
            subscription_qos: QoS::AtLeastOnce,
            msg: TopicMessage {
                retain: false,
                qos: QoS::AtMostOnce,
                payload: vec![],
            },
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum PublishEvent {
    Publish(Publish),
    PubRec(PubResp),
    PubRel(PubResp),
    PubComp(PubResp),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SessionState {
    pub peer: SocketAddr,
}

trait Subscriptions {
    fn store_subscription(&self, topic_filter: &[u8], option: &SubscriptionOptions) -> Result<()>;
    fn remove_subscription(&self, topic_filter: &[u8]) -> Result<bool>;
    fn list_subscriptions(&self) -> Result<Vec<(IVec, SubscriptionOptions)>>;
    fn clear_subscriptions(&self) -> Result<usize>;
}

#[derive(Debug)]
pub(crate) struct Session {
    id: MqttString,
    response_tx: Sender<ControlPacket>,
    subscription_tx: Sender<SubscriptionEvent>,
    peer: SocketAddr,
    properties: ConnectProperties,
    will: Option<Will>,
    expire_interval: Duration,
    config: ConnectionSettings,
    server_flows: HashMap<u16, Sender<PublishEvent>>,
    client_flows: HashMap<u16, Sender<PublishEvent>>,
    topics: Arc<Topics>,
    packet_identifier_seq: Arc<AtomicU16>,
    sessions_db: Tree,
    subscriptions_db: Tree,
    topic_filters: HashMap<String, Vec<oneshot::Sender<()>>>,
}

async fn publish_topic(root: Arc<Topics>, msg: Publish) -> Result<()> {
    let topic = std::str::from_utf8(&msg.topic_name[..])?;

    let message = TopicMessage {
        retain: msg.retain,
        qos: msg.qos,
        //properties: msg.properties,
        payload: msg.payload.to_vec(),
    };

    root.publish(topic, message).await
}

#[instrument(skip(rx, response_tx), err)]
async fn exactly_once_client(
    session: MqttString,
    packet_identifier: u16,
    mut rx: Receiver<PublishEvent>,
    response_tx: Sender<ControlPacket>,
) -> Result<()> {
    if let Some(event) = rx.recv().await {
        if let PublishEvent::PubRec(_msg) = event {
            let rel = ControlPacket::PubRel(PubResp {
                packet_type: PacketType::PUBREL,
                packet_identifier,
                reason_code: ReasonCode::Success,
                properties: Default::default(),
            });
            response_tx.send(rel).await?;

            match rx.recv().await {
                Some(PublishEvent::PubComp(comp)) => {
                    trace!("{:?}", comp);
                }
                Some(event) => bail!("{:?} unknown event received: {:?}", session, event),
                None => bail!("{:?} channel closed", session),
            }
        } else {
            bail!("{:?} unexpected pubrec event {:?}", session, event);
        }
    }
    Ok(())
}

#[instrument(skip(rx, response_tx, root), err)]
async fn exactly_once_server(
    session: MqttString,
    packet_identifier: u16,
    root: Arc<Topics>,
    mut rx: Receiver<PublishEvent>,
    response_tx: Sender<ControlPacket>,
) -> Result<()> {
    if let Some(event) = rx.recv().await {
        if let PublishEvent::Publish(msg) = event {
            //TODO handler error
            publish_topic(root, msg).await?;
            let rec = ControlPacket::PubRec(PubResp {
                packet_type: PacketType::PUBREC,
                packet_identifier,
                reason_code: ReasonCode::Success,
                properties: Default::default(),
            });
            response_tx.send(rec).await?;

            match rx.recv().await {
                Some(PublishEvent::PubRel(rel)) => {
                    trace!("{:?}", rel);
                    //TODO discard packet identifier
                    let comp = ControlPacket::PubComp(PubResp {
                        packet_type: PacketType::PUBCOMP,
                        packet_identifier,
                        reason_code: ReasonCode::Success,
                        properties: Default::default(),
                    });
                    response_tx.send(comp).await?;
                }
                Some(event) => bail!("{:?} unknown event received: {:?}", session, event),
                None => bail!("{:?} channel closed", session),
            }
        } else {
            bail!("{:?} unexpected publish event {:?}", session, event);
        }
    }
    Ok(())
}

impl SessionState {
    pub fn new(peer: SocketAddr) -> Self {
        SessionState { peer }
    }
}

impl Session {
    pub fn new(
        id: MqttString,
        response_tx: Sender<ControlPacket>,
        subscription_tx: Sender<SubscriptionEvent>,
        peer: SocketAddr,
        properties: ConnectProperties,
        will: Option<Will>,
        config: ConnectionSettings,
        topic_manager: Arc<Topics>,
        sessions_db: Tree,
        subscriptions_db: Tree,
    ) -> Self {
        info!("new session: {:?}", id);
        Self {
            id,
            //created: Instant::now(),
            response_tx,
            subscription_tx,
            peer,
            properties,
            will,
            expire_interval: Duration::from_millis(1),
            config,
            server_flows: HashMap::new(),
            client_flows: HashMap::new(),
            topics: topic_manager,
            packet_identifier_seq: Arc::new(AtomicU16::new(1)),
            sessions_db,
            subscriptions_db,
            topic_filters: HashMap::new(),
        }
    }

    //#[instrument(skip(self))]
    pub fn acquire(&self) -> Result<bool> {
        let update_fn = |old: Option<&[u8]>| -> Option<Vec<u8>> {
            let session = match old {
                Some(encoded) => {
                    if let Ok(mut session) = SessionState::try_from(encoded) {
                        session.peer = self.peer;
                        Some(session)
                    } else {
                        Some(SessionState::new(self.peer))
                    }
                }
                None => Some(SessionState::new(self.peer)),
            };
            session.and_then(|s: SessionState| s.try_into().ok())
        };

        if let Some(_encoded) = self
            .sessions_db
            .fetch_and_update(self.id.clone(), update_fn)
            .map_err(Error::msg)?
        {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /*
        #[instrument(skip(self, session_event_rx, connection_context_rx), fields(identifier = field::display(& self.id)),
        err)]
        pub(crate) async fn session(
            &mut self,
            mut session_event_rx: Receiver<SessionEvent>,
            mut connection_context_rx: Receiver<ConnectionContextState>,
        ) -> Result<()> {
            trace!("start");
            loop {
                tokio::select! {
                                res = timeout(self.expire_interval, connection_context_rx.recv()), if self
                                .connection.is_none()  => {
                                    match res {
                                        Ok(Some((response_tx, prop, will, clean_start))) => {
                                            debug!("new connection context");
                                            if let Some(ex) = prop.session_expire_interval {
                                                self.expire_interval = Duration::from_secs(ex as u64);
                                            }
                                            self.clear();
                                            self.connection = Some((response_tx, prop, will, clean_start));
                                        },
                                        Ok(None) => {
                                            bail!("connection context closed. expire {}", self.expire_interval
                                            .as_secs());
                                        }
                                        Err(e) => {
                                            debug!("session expired: {}", e);
                                            break;
                                        }
                                    }
                                }
                                Some(msg) = session_event_rx.recv(), if self.connection.is_some() => {
                                    trace!("session event: {:?}", msg);
                                    match msg {
                                        SessionEvent::Publish(p) => self.publish(p).await?,
                                        SessionEvent::PubAck(p) => self.puback(p).await?,
                                        SessionEvent::PubRec(p) => self.pubrec(p).await?,
                                        SessionEvent::PubRel(p) => self.pubrel(p).await?,
                                        SessionEvent::PubComp(p) => self.pubcomp(p).await?,
                                        SessionEvent::Subscribe(s) => self.subscribe(s).await?,
                                        SessionEvent::SubAck(s) => self.suback(s).await?,
                                        SessionEvent::UnSubscribe(s) => self.unsubscribe(s).await?,
                                        SessionEvent::UnSubAck(s) => self.unsuback(s).await?,
                                        SessionEvent::Disconnect(d) => {
                                            if let Some(ex) = d.properties.session_expire_interval {
                                                self.expire_interval = Duration::from_secs(ex as u64);
                                            }
                                            self.disconnect(d).await?;
                                            self.connection = None;
                                        }
                                    }
                                },
                                /*Some(event) = new_topic_stream.next() => {
                                    match event {
                                        Ok((topic, topic_tx, retained)) => {
                                            trace!("new topic event {:?}", topic);
                                            for (filter, option) in self.filtered(topic.as_str()) {
                                                debug!("new topic: {} match topic filter {}", topic, filter);
                                                let (unsubscribe_tx, unsubscribe_rx) = oneshot::channel();
                                                self.topic_filters.entry((filter.to_owned(), option.to_owned())).and_modify(|v| v.push(unsubscribe_tx));

                                                let subscription = Subscription::new(
                                                    self.id.to_owned(),
                                                    option.to_owned(),
                                                    subscription_event_tx.clone(),
                                                );

                                                let topic_name = topic.to_owned();
                                                let stream = topic_tx.subscribe().into_stream();
                                                tokio::spawn(async move {
                                                   subscription.subscribe(topic_name, stream, unsubscribe_rx).await
                                                });

                                                let ret_msg = (filter, option, retained.clone());
                                                subscription_event_tx.send(ret_msg).await.map_err(Error::msg)?;
                                            }
                                        },
                                        Err(Lagged(lag)) => {
                                            warn!("new topic event lagged: {}", lag);
                                        },
                                        Err(err) => {
                                            warn!(cause = ?err, "new topic event error: ");
                                        }
                                    }
                                }*/
                /*                Some((topic_filter, option, message)) = subscription_event_rx.recv(), if self
                                .connection.is_some() => {
                                    trace!("subscription publish topic: {}, message: {:?}", topic_filter, message);
                                    self.publish_client(option, message).await?
                                },*/
                                else => break,
                            }
            }
            trace!("stop");
            Ok(())
        }
    */
    /*    fn filtered(&self, topic: &str) -> Vec<(String, SubscriptionOptions)> {
        self.topic_filters
            .keys()
            .filter(|(filter, _)| Topics::match_filter(topic, filter.as_str()))
            .map(|(f, o)| (f.to_owned(), o.to_owned()))
            .collect()
    }*/

    #[instrument(skip(self), fields(identifier = field::debug(& self.id)), err)]
    async fn publish_client(&mut self, event: SubscriptionEvent) -> Result<()> {
        let msg = event.msg;
        let packet_identifier = self.packet_identifier_seq.clone();
        let qos = min(msg.qos, event.subscription_qos);
        let packet_identifier = if QoS::AtMostOnce == qos {
            None
        } else {
            let id = packet_identifier.fetch_add(1, Ordering::SeqCst);
            if id == 0 {
                let id = packet_identifier.fetch_add(1, Ordering::SeqCst);
                Some(id)
            } else {
                Some(id)
            }
        };
        let publish = ControlPacket::Publish(Publish {
            dup: false,
            qos,
            retain: msg.retain,
            topic_name: Default::default(),
            packet_identifier,
            properties: PublishProperties::default(),
            payload: Bytes::from(msg.payload),
        });

        if let Some(maximum_packet_size) = self.properties.maximum_packet_size {
            if publish.encoded_size() > maximum_packet_size as usize {
                warn!("exceed maximum_packet_size: {:?}", publish);
                return Ok(());
            }
        }

        self.response_tx.send(publish).await.map_err(Error::msg)?;

        if QoS::ExactlyOnce == qos {
            let (tx, rx) = mpsc::channel(1);
            let packet_identifier = packet_identifier.unwrap();
            self.client_flows.insert(packet_identifier, tx);
            let response_tx = self.response_tx.clone();
            let session = self.id.clone();
            tokio::spawn(async move {
                if let Err(err) =
                    exactly_once_client(session.to_owned(), packet_identifier, rx, response_tx)
                        .await
                {
                    error!(cause = ?err, "QoS 2 protocol error: {}", err);
                }
            });
        };
        Ok(())
    }

    pub async fn handle_msg(&mut self, msg: ControlPacket) -> Result<()> {
        match msg {
            ControlPacket::Connect(p) => self.connect(p).await,
            ControlPacket::Publish(p) => self.publish(p).await,
            ControlPacket::PubAck(p) => self.puback(p).await,
            ControlPacket::PubRec(p) => self.pubrec(p).await,
            ControlPacket::PubRel(p) => self.pubrel(p).await,
            ControlPacket::PubComp(p) => self.pubcomp(p).await,
            ControlPacket::Subscribe(s) => self.subscribe(s).await,
            ControlPacket::SubAck(s) => self.suback(s).await,
            ControlPacket::UnSubscribe(s) => self.unsubscribe(s).await,
            ControlPacket::UnSubAck(s) => self.unsuback(s).await,
            ControlPacket::Disconnect(d) => {
                if let Some(ex) = d.properties.session_expire_interval {
                    self.expire_interval = Duration::from_secs(ex as u64);
                }
                self.disconnect(d).await
                //self.connection = None;
            }
            _ => unreachable!(),
        }
    }

    pub async fn handle_event(&mut self, event: SubscriptionEvent) -> Result<()> {
        self.publish_client(event).await
    }

    #[instrument(skip(self), fields(identifier = field::debug(& self.id)), err)]
    fn init(&mut self, clean_start: bool) -> Result<bool> {
        let session_present = self.acquire().context("acquire session")?;

        if session_present {
            if clean_start {
                let removed = self.clear_subscriptions().context("clean start session")?;
                info!("removed {} subscriptions", removed);
            } else {
                // load session state
                let subscriptions = self.list_subscriptions().context("get subscriptions")?;
                for (topic_filter, option) in subscriptions {
                    debug!("subscribe {:?}:{:?}", topic_filter, option);
                }
                // start subscriptions
            }
        };

        Ok(session_present)
    }

    #[instrument(skip(self, msg), fields(identifier = field::debug(& self.id)), err)]
    async fn connect(&mut self, msg: Connect) -> Result<()> {
        match self.init(msg.clean_start_flag) {
            Ok(session_present) => {
                trace!("session_present: {}", session_present);
                let assigned_client_identifier = if let Some(_) = msg.client_identifier.clone() {
                    None
                } else {
                    Some(self.id.clone())
                };

                let maximum_qos = if let Some(QoS::ExactlyOnce) = self.config.maximum_qos {
                    None
                } else {
                    self.config.maximum_qos
                };

                let session_expire_interval =
                    if let Some(server_expire) = self.config.session_expire_interval {
                        if let Some(client_expire) = msg.properties.session_expire_interval {
                            Some(min(server_expire, client_expire))
                        } else {
                            Some(server_expire)
                        }
                    } else {
                        None
                    };

                let ack = ControlPacket::ConnAck(ConnAck {
                    session_present,
                    reason_code: ReasonCode::Success,
                    properties: ConnAckProperties {
                        session_expire_interval,
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
                self.response_tx.send(ack).await.map_err(Error::msg)
            }
            Err(err) => {
                warn!(cause = ?err, "session error: ");
                let ack = ControlPacket::ConnAck(ConnAck {
                    session_present: false,
                    reason_code: ReasonCode::UnspecifiedError,
                    properties: ConnAckProperties::default(),
                });
                self.response_tx.send(ack).await?;
                // disconnect
                return Err(err);
            }
        }
    }

    #[instrument(skip(self, msg), fields(identifier = field::debug(& self.id)), err)]
    async fn publish(&mut self, msg: Publish) -> Result<()> {
        match msg.qos {
            //TODO handler error
            QoS::AtMostOnce => publish_topic(self.topics.clone(), msg).await?,
            QoS::AtLeastOnce => {
                if let Some(packet_identifier) = msg.packet_identifier {
                    //TODO handler error
                    publish_topic(self.topics.clone(), msg).await?;
                    let ack = ControlPacket::PubAck(PubResp {
                        packet_type: PacketType::PUBACK,
                        packet_identifier,
                        reason_code: ReasonCode::Success,
                        properties: PubResProperties {
                            reason_string: None,
                            user_properties: vec![],
                        },
                    });
                    //trace!("send {:?}", ack);
                    self.response_tx.send(ack).await?
                } else {
                    bail!("undefined packet_identifier");
                }
            }
            QoS::ExactlyOnce => {
                if let Some(packet_identifier) = msg.packet_identifier {
                    if self.server_flows.contains_key(&packet_identifier) && !msg.dup {
                        let disconnect = ControlPacket::Disconnect(Disconnect {
                            reason_code: ReasonCode::PacketIdentifierInUse,
                            properties: Default::default(),
                        });
                        self.response_tx.send(disconnect).await?;
                    } else if self.server_flows.len()
                        < self.config.receive_maximum.unwrap() as usize
                    {
                        let session = self.id.clone();
                        let (tx, rx) = mpsc::channel(1);
                        let root = self.topics.clone();
                        let response_tx = self.response_tx.clone();
                        tokio::spawn(async move {
                            if let Err(err) = exactly_once_server(
                                session,
                                packet_identifier,
                                root,
                                rx,
                                response_tx,
                            )
                            .await
                            {
                                error!(cause = ?err, "QoS 2 protocol error: {}", err);
                            }
                        });
                        self.server_flows.insert(packet_identifier, tx.clone());
                        tx.send(PublishEvent::Publish(msg)).await?
                    } else {
                        let disconnect = ControlPacket::Disconnect(Disconnect {
                            reason_code: ReasonCode::ReceiveMaximumExceeded,
                            properties: Default::default(),
                        });
                        self.response_tx.send(disconnect).await?;
                    }
                } else {
                    bail!("undefined packet_identifier");
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn puback(&self, msg: PubResp) -> Result<()> {
        trace!("{:?}", msg);
        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn pubrel(&self, msg: PubResp) -> Result<()> {
        if let Some(tx) = self.server_flows.get(&msg.packet_identifier) {
            tx.clone()
                .send(PublishEvent::PubRel(msg))
                .await
                .map_err(Error::msg)
        } else {
            Err(anyhow!(
                "protocol flow error: not found flow for packet identifier {}",
                msg.packet_identifier
            ))
        }
    }

    #[instrument(skip(self), err)]
    async fn pubrec(&self, msg: PubResp) -> Result<()> {
        if let Some(tx) = self.client_flows.get(&msg.packet_identifier) {
            tx.clone()
                .send(PublishEvent::PubRec(msg))
                .await
                .map_err(Error::msg)
        } else {
            Err(anyhow!(
                "protocol flow error: not found flow for packet identifier {}",
                msg.packet_identifier
            ))
        }
    }

    #[instrument(skip(self), err)]
    async fn pubcomp(&self, msg: PubResp) -> Result<()> {
        if let Some(tx) = self.client_flows.get(&msg.packet_identifier) {
            tx.clone()
                .send(PublishEvent::PubComp(msg))
                .await
                .map_err(Error::msg)
        } else {
            Err(anyhow!(
                "protocol flow error: not found flow for packet identifier {}",
                msg.packet_identifier
            ))
        }
    }

    #[instrument(skip(subscriber, subscription_tx))]
    pub(crate) async fn subscription(
        session: MqttString,
        option: SubscriptionOptions,
        subscription_tx: Sender<SubscriptionEvent>,
        topic_name: &str,
        mut subscriber: Subscriber,
    ) -> Result<()> {
        //   tokio::pin!(stream);
        trace!("start");
        while let Some(event) = (&mut subscriber).await {
            debug!("receive subscription event {:?}", event);
            match event {
                Event::Insert { key, value } => match key.as_ref().read_u64::<BigEndian>() {
                    Ok(id) => {
                        let msg = bincode::deserialize(&value).context("deserialize event")?;
                        debug!("id: {}, value: {:?}", id, msg);
                        subscription_tx
                            .send(SubscriptionEvent {
                                id,
                                subscription_qos: option.qos,
                                msg,
                            })
                            .await
                            .context("subscription send")?
                    }
                    Err(_) => warn!("invalid log id: {:#x?}", key),
                },
                Event::Remove { .. } => unreachable!(),
            }
        }

        trace!("end");
        Ok(())
    }

    async fn subscribe_topic(
        &mut self,
        topic_filter: MqttString,
        options: SubscriptionOptions,
    ) -> Result<ReasonCode> {
        let topic_filter = std::str::from_utf8(&topic_filter[..])?;
        let channels = self.topics.subscribe(topic_filter).await?;
        trace!("subscribe returns {} subscriptions", channels.len());
        let reason_code = match options.qos {
            QoS::AtMostOnce => ReasonCode::Success,
            QoS::AtLeastOnce => ReasonCode::GrantedQoS1,
            QoS::ExactlyOnce => ReasonCode::GrantedQoS2,
        };
        // add a subscription record in sled db
        self.store_subscription(topic_filter.as_ref(), options.borrow())?;

        let subscriptions = self
            .topic_filters
            .entry(topic_filter.to_owned())
            .or_insert(vec![]);

        for (topic, subscriber) in channels {
            let (unsubscribe_tx, unsubscribe_rx) = oneshot::channel();
            subscriptions.push(unsubscribe_tx);
            let session = self.id.to_owned();
            let subscription_tx = self.subscription_tx.clone();
            let options = options.to_owned();
            tokio::spawn(async move {
                tokio::select! {
                    Err(err) = Self::subscription(session, options, subscription_tx, topic
                    .as_str(),
                    subscriber) => {
                        warn!(cause = ? err, "subscription error");
                    },
                    _ = unsubscribe_rx => {
                        debug!("unsubscribe");
                    }
                }
            });
        }

        Ok(reason_code)
    }

    #[instrument(skip(self, msg), err)]
    async fn subscribe(&mut self, msg: Subscribe) -> Result<()> {
        debug!("subscribe topic filters: {:?}", msg.topic_filters);
        let mut reason_codes = Vec::new();
        for (topic_filter, options) in msg.topic_filters {
            reason_codes.push(match self.subscribe_topic(topic_filter, options).await {
                Ok(reason_code) => reason_code,
                Err(err) => {
                    warn!(cause = ?err, "subscribe error: ");
                    ReasonCode::TopicFilterInvalid
                }
            });
        }

        let suback = ControlPacket::SubAck(SubAck {
            packet_identifier: msg.packet_identifier,
            properties: Default::default(),
            reason_codes,
        });
        self.response_tx.send(suback).await.map_err(Error::msg)
        // trace!("send {:?}", suback);
    }

    #[instrument(skip(self), err)]
    async fn suback(&self, msg: SubAck) -> Result<()> {
        trace!("{:?}", msg);
        Ok(())
    }

    #[instrument(skip(self, msg), err)]
    async fn unsubscribe(&mut self, msg: UnSubscribe) -> Result<()> {
        debug!("topic filters: {:?}", msg.topic_filters);
        let mut reason_codes = Vec::new();
        for topic_filter in msg.topic_filters {
            reason_codes.push(match std::str::from_utf8(&topic_filter[..]) {
                Ok(topic_filter) => {
                    self.topic_filters
                        .remove(topic_filter)
                        .map(|removed| debug!("stop {} subscriptions", removed.len()));
                    self.remove_subscription(topic_filter.as_ref())?;
                    ReasonCode::Success
                }
                Err(err) => {
                    debug!(cause = ?err, "un subscribe error: ");
                    ReasonCode::TopicFilterInvalid
                }
            })
        }

        let unsuback = ControlPacket::UnSubAck(SubAck {
            packet_identifier: msg.packet_identifier,
            properties: Default::default(),
            reason_codes,
        });
        self.response_tx.send(unsuback).await.map_err(Error::msg)
        // trace!("send {:?}", suback);
    }

    #[instrument(skip(self), err)]
    async fn unsuback(&self, msg: SubAck) -> Result<()> {
        trace!("{:?}", msg);
        Ok(())
    }

    #[instrument(skip(self, msg))]
    async fn disconnect(&self, msg: Disconnect) -> Result<()> {
        if msg.reason_code != ReasonCode::Success {
            // publish will
            if let Some(will) = self.will.borrow() {
                debug!("send will: {:?}", will);
                publish_topic(
                    self.topics.clone(),
                    Publish {
                        dup: false,
                        qos: will.qos,
                        retain: will.retain,
                        topic_name: will.topic.to_owned(),
                        packet_identifier: None,
                        properties: PublishProperties {
                            payload_format_indicator: will.properties.payload_format_indicator,
                            message_expire_interval: will.properties.message_expire_interval,
                            topic_alias: None,
                            response_topic: will.properties.response_topic.to_owned(),
                            correlation_data: will.properties.correlation_data.to_owned(),
                            user_properties: will.properties.user_properties.to_owned(),
                            subscription_identifier: None,
                            content_type: will.properties.content_type.to_owned(),
                        },
                        payload: will.payload.to_owned(),
                    },
                )
                .await?
            }
        }

        Ok(())
    }
}

impl Subscriptions for Session {
    fn store_subscription(&self, topic_filter: &[u8], option: &SubscriptionOptions) -> Result<()> {
        let key = SubscriptionKey {
            session: self.id.as_ref(),
            topic_filter,
        };
        self.subscriptions_db
            .insert(bincode::serialize(&key)?, bincode::serialize(&option)?)
            .map(|_| ())
            .map_err(Error::msg)
    }

    fn remove_subscription(&self, topic_filter: &[u8]) -> Result<bool> {
        let key = SubscriptionKey {
            session: self.id.as_ref(),
            topic_filter,
        };
        self.subscriptions_db
            .remove(bincode::serialize(&key)?)
            .map(|d| d.is_some())
            .map_err(Error::msg)
    }

    fn list_subscriptions(&self) -> Result<Vec<(IVec, SubscriptionOptions)>, Error> {
        let key = SubscriptionKey {
            session: self.id.as_ref(),
            topic_filter: "".as_ref(),
        };
        let prefix = bincode::serialize(&key)?;
        let mut ret = vec![];

        for res in self.subscriptions_db.scan_prefix(prefix) {
            match res {
                Ok((key, value)) => ret.push((key, SubscriptionOptions::try_from(value)?)),
                Err(err) => warn!(cause = ?err, "subscription scan error:"),
            }
        }

        Ok(ret)
    }

    fn clear_subscriptions(&self) -> Result<usize> {
        let key = SubscriptionKey {
            session: self.id.as_ref(),
            topic_filter: "".as_ref(),
        };
        let mut batch = Batch::default();
        let prefix = bincode::serialize(&key)?;
        let mut ret = 0;

        for res in self.subscriptions_db.scan_prefix(prefix) {
            match res {
                Ok((key, _)) => {
                    batch.remove(key);
                    ret += 1;
                }
                Err(err) => warn!(cause = ?err, "subscription scan error:"),
            }
        }
        self.subscriptions_db
            .apply_batch(batch)
            .map(|_| ret)
            .map_err(Error::msg)
    }
}

impl TryFrom<IVec> for SessionState {
    type Error = Error;

    fn try_from(encoded: IVec) -> Result<Self> {
        bincode::deserialize(encoded.as_ref()).map_err(Error::msg)
    }
}

impl TryFrom<&[u8]> for SessionState {
    type Error = Error;

    fn try_from(encoded: &[u8]) -> Result<Self> {
        bincode::deserialize(encoded.as_ref()).map_err(Error::msg)
    }
}

impl TryInto<Vec<u8>> for SessionState {
    type Error = Error;

    fn try_into(self) -> Result<Vec<u8>> {
        bincode::serialize(&self).map_err(Error::msg)
    }
}

impl Drop for Session {
    #[instrument(skip(self), fields(identifier = field::debug(& self.id)))]
    fn drop(&mut self) {
        info!("session closed");
    }
}
