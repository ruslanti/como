use std::borrow::Borrow;
use std::cmp::min;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;

use anyhow::{anyhow, Context, Error, Result};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sled::Tree;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Duration};
use tracing::{debug, error, field, info, instrument, trace, warn};

use como_mqtt::identifier::PacketIdentifier;
use como_mqtt::v5::property::{
    ConnAckProperties, ConnectProperties, PublishProperties, ResponseProperties,
};
use como_mqtt::v5::string::MqttString;
use como_mqtt::v5::types::{
    ConnAck, Connect, ControlPacket, Disconnect, Publish, PublishResponse, QoS, ReasonCode, SubAck,
    Subscribe, SubscriptionOptions, UnSubscribe, Will,
};

use crate::context::{
    subscribe_topic, SessionContext, SessionState, SessionStore, SubscriptionsStore,
};
use crate::exactly_once::{exactly_once_client, exactly_once_server};
use crate::metric;
use crate::topic::{NewTopicSubscriber, PubMessage, TopicManager};

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicMessage {
    id: u64,
    topic_name: String,
    option: SubscriptionOptions,
    msg: PubMessage,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SessionEvent {
    SessionTakenOver(SessionState),
    TopicMessage(TopicMessage),
}

#[derive(Debug, Eq, PartialEq)]
pub enum PublishEvent {
    PubRec(PublishResponse),
    PubRel(PublishResponse),
    PubComp(PublishResponse),
}

pub(crate) type SubscribedTopics = HashMap<String, oneshot::Sender<()>>;
pub(crate) type SubscriptionValue = (SubscriptionOptions, SubscribedTopics);

pub(crate) struct Session {
    unique_id: u64,
    client_id: String,
    response_tx: Sender<ControlPacket>,
    session_event_tx: Sender<SessionEvent>,
    session_event_rx: Receiver<SessionEvent>,
    peer: SocketAddr,
    properties: ConnectProperties,
    will: Option<Will>,
    session_expire_interval: Option<u32>,
    server_flows: BTreeMap<u16, Sender<PublishEvent>>,
    client_flows: BTreeMap<u16, Sender<PublishEvent>>,
    packet_identifier: PacketIdentifier,
    context: SessionContext,
    topic_filters: HashMap<String, SubscriptionValue>,
    new_topic_subscriber: NewTopicSubscriber,
}

impl TopicMessage {
    pub fn new(id: u64, topic_name: String, option: SubscriptionOptions, msg: PubMessage) -> Self {
        TopicMessage {
            id,
            topic_name,
            option,
            msg,
        }
    }
}

impl Session {
    pub fn new(
        id: String,
        response_tx: Sender<ControlPacket>,
        peer: SocketAddr,
        properties: ConnectProperties,
        will: Option<Will>,
        context: SessionContext,
    ) -> Self {
        info!("new session: {:?}", id);
        metric::ACTIVE_SESSIONS.with_label_values(&[]).inc();

        let (session_event_tx, session_event_rx) = mpsc::channel(32);
        let topic_event = context.watch_new_topic();
        Self {
            unique_id: context.generate_id(),
            client_id: id,
            //created: Instant::now(),
            response_tx,
            session_event_tx,
            session_event_rx,
            peer,
            properties,
            will,
            session_expire_interval: None,
            server_flows: BTreeMap::new(),
            client_flows: BTreeMap::new(),
            packet_identifier: Default::default(),
            context,
            topic_filters: HashMap::new(),
            new_topic_subscriber: topic_event,
        }
    }

    #[instrument(skip(self), fields(client_id = field::debug(& self.client_id)))]
    async fn publish_client(&mut self, event: TopicMessage) -> Option<ControlPacket> {
        let msg = event.msg;
        let qos = min(msg.qos, event.option.qos);
        let packet_identifier = if QoS::AtMostOnce == qos {
            None
        } else {
            self.packet_identifier.next()
        };

        let publish = Publish {
            dup: false,
            qos,
            retain: msg.retain,
            topic_name: MqttString::from(event.topic_name),
            packet_identifier,
            properties: PublishProperties::default(),
            payload: Bytes::from(msg.payload),
        };

        if let Some(maximum_packet_size) = self.properties.maximum_packet_size {
            if publish.size() > maximum_packet_size as usize {
                warn!("exceed maximum_packet_size: {}", publish);
                return None;
            }
        }

        if QoS::ExactlyOnce == qos {
            let packet_identifier = packet_identifier.unwrap();
            let (tx, rx) = mpsc::channel(1);
            self.client_flows.insert(packet_identifier, tx);
            let response_tx = self.response_tx.clone();
            let client_id = self.client_id.clone();
            tokio::spawn(async move {
                if let Err(error) =
                    exactly_once_client(client_id, packet_identifier, rx, response_tx).await
                {
                    error!(cause = ?error, "QoS 2 protocol error");
                }
            });
        };

        Some(ControlPacket::Publish(publish))
    }

    pub(crate) async fn handle_msg(&mut self, msg: ControlPacket) -> Result<Option<ControlPacket>> {
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
            ControlPacket::Disconnect(d) => self.disconnect(d).await,
            _ => unreachable!(),
        }
    }

    async fn handle_topic_event(&mut self, topic_name: String, log: Tree) {
        debug!("new topic event: {}", topic_name);
        let subscription_tx = self.session_event_tx.clone();
        let client_id = self.client_id.to_owned();
        for (topic_filter, (options, unsubscribes)) in self.topic_filters.iter_mut() {
            match TopicManager::match_filter(topic_name.as_str(), topic_filter.as_str()) {
                Ok(true) => {
                    if !unsubscribes.contains_key(&topic_name) {
                        debug!(
                            "topic: {} match topic filter: {}",
                            topic_name.as_str(),
                            topic_filter.as_str()
                        );

                        if let Some((key, value)) = log.last().ok().flatten() {
                            let id = key.as_ref().read_u64::<BigEndian>().unwrap_or(0);
                            if let Ok(msg) = bincode::deserialize::<PubMessage>(value.as_ref())
                                .context("deserialize event")
                            {
                                if let Err(err) = self
                                    .session_event_tx
                                    .send(SessionEvent::TopicMessage(TopicMessage::new(
                                        id,
                                        topic_name.to_owned(),
                                        options.clone(),
                                        msg,
                                    )))
                                    .await
                                {
                                    warn!(cause = ?err, "subscription send");
                                }
                            }
                        };

                        unsubscribes.insert(
                            topic_name.to_owned(),
                            subscribe_topic(
                                client_id.to_owned(),
                                topic_name.to_owned(),
                                options.to_owned(),
                                subscription_tx.clone(),
                                log.watch_prefix(vec![]),
                            ),
                        );
                    } else {
                        debug!(
                            "already subscribed topic: {} match topic filter: {}",
                            topic_name.as_str(),
                            topic_filter.as_str()
                        );
                    }
                }
                Err(error) => warn!(cause = ?error, "match filter: "),
                _ => {
                    /* doesn't match */
                    trace!("{} doesn't match {}", topic_name, topic_filter);
                }
            }
        }
    }

    #[instrument(skip(self), fields(client_id = field::debug(& self.client_id)), err)]
    pub async fn session(&mut self) -> Result<Option<ControlPacket>> {
        tokio::select! {
            res = self.session_event_rx.recv() => {
                if let Some(event) = res {
                    trace!("subscription event: {:?}", event);
                    let response = match event {
                        SessionEvent::SessionTakenOver(_taken) =>
                            Some(ControlPacket::Disconnect(Disconnect {reason_code: ReasonCode::SessionTakenOver, properties: Default::default()}))
                        ,
                        SessionEvent::TopicMessage(message) => self
                            .publish_client(message)
                            .await,
                    };
                    return Ok(response);
                }
            }
            res = self.new_topic_subscriber.recv() => {
                match res {
                    Ok((topic_name, log)) => self.handle_topic_event(topic_name, log).await,
                    Err(err) => warn!(cause=?err, "new topic event recv error")
                }
            }
        }
        Ok(None)
    }

    #[instrument(skip(self), fields(client_id = field::debug(& self.client_id)), err)]
    fn init(&mut self, clean_start: bool) -> Result<bool> {
        let session_state = self
            .context
            .acquire(
                self.unique_id,
                self.client_id.as_str(),
                self.peer,
                clean_start,
            )
            .context("acquire session")?;
        self.context.start_monitor(
            self.unique_id,
            self.client_id.as_str(),
            self.session_event_tx.clone(),
        );

        if let Some(_session_state) = session_state {
            if clean_start {
                let removed = self
                    .context
                    .clear_subscriptions(self.client_id.as_str())
                    .context("clean start session")?;
                info!("removed {} subscriptions", removed);
            } else {
                // load session state
                let subscriptions = self
                    .context
                    .list_subscriptions(self.client_id.as_str())
                    .context("list subscriptions")?;
                for (topic_filter, option) in subscriptions {
                    debug!("subscribe {:?}:{:?}", topic_filter, option);
                }
                // start subscriptions
            }
            Ok(true) // session present
        } else {
            Ok(false) // new session
        }
    }

    #[instrument(skip(self, msg), fields(client_id = field::debug(& self.client_id)), err)]
    async fn connect(&mut self, msg: Connect) -> Result<Option<ControlPacket>> {
        match self.init(msg.clean_start_flag) {
            Ok(session_present) => {
                trace!("session_present: {}", session_present);
                let assigned_client_identifier = if msg.client_identifier.is_some() {
                    None
                } else {
                    Some(MqttString::from(self.client_id.to_owned()))
                };

                let maximum_qos = if let Some(QoS::ExactlyOnce) =
                    self.context.settings().connection.maximum_qos
                {
                    None
                } else {
                    self.context.settings().connection.maximum_qos
                };

                let session_expire_interval = if let Some(server_expire) =
                    self.context.settings().connection.session_expire_interval
                {
                    if let Some(client_expire) = msg.properties.session_expire_interval {
                        Some(min(server_expire, client_expire))
                    } else {
                        Some(server_expire)
                    }
                } else {
                    None
                };
                self.session_expire_interval =
                    session_expire_interval.or(msg.properties.session_expire_interval);
                self.will = msg.will;

                let ack = ControlPacket::ConnAck(ConnAck {
                    session_present,
                    reason_code: ReasonCode::Success,
                    properties: ConnAckProperties {
                        session_expire_interval,
                        receive_maximum: self.context.settings().connection.receive_maximum,
                        maximum_qos,
                        retain_available: self.context.settings().connection.retain_available,
                        maximum_packet_size: self.context.settings().connection.maximum_packet_size,
                        assigned_client_identifier,
                        topic_alias_maximum: self.context.settings().connection.topic_alias_maximum,
                        reason_string: None,
                        user_properties: vec![],
                        wildcard_subscription_available: None,
                        subscription_identifier_available: None,
                        shared_subscription_available: None,
                        server_keep_alive: self.context.settings().connection.server_keep_alive,
                        response_information: None,
                        server_reference: None,
                        authentication_method: None,
                        authentication_data: None,
                    },
                });
                Ok(Some(ack))
            }
            Err(err) => {
                warn!(cause = ?err, "session error: ");
                let ack = ControlPacket::ConnAck(ConnAck {
                    session_present: false,
                    reason_code: ReasonCode::UnspecifiedError,
                    properties: ConnAckProperties::default(),
                });
                Ok(Some(ack))
                //self.response_tx.send(ack).await?;
            }
        }
    }

    #[instrument(skip(self, msg), fields(client_id = field::debug(& self.client_id)), err)]
    async fn publish(&mut self, msg: Publish) -> Result<Option<ControlPacket>> {
        match msg.qos {
            //TODO handler error
            QoS::AtMostOnce => {
                self.context.publish(msg).await?;
                Ok(None)
            }
            QoS::AtLeastOnce => {
                if let Some(packet_identifier) = msg.packet_identifier {
                    //TODO handler error
                    self.context.publish(msg).await?;
                    let ack = ControlPacket::PubAck(PublishResponse {
                        packet_identifier,
                        reason_code: ReasonCode::Success,
                        properties: ResponseProperties {
                            reason_string: None,
                            user_properties: vec![],
                        },
                    });
                    Ok(Some(ack))
                } else {
                    Err(anyhow!("undefined packet_identifier"))
                }
            }
            QoS::ExactlyOnce => {
                if let Some(packet_identifier) = msg.packet_identifier {
                    if self.server_flows.contains_key(&packet_identifier) && !msg.dup {
                        Ok(Some(ControlPacket::Disconnect(Disconnect {
                            reason_code: ReasonCode::PacketIdentifierInUse,
                            properties: Default::default(),
                        })))
                    } else if self.server_flows.len()
                        < self.context.settings().connection.receive_maximum.unwrap() as usize
                    {
                        self.context.publish(msg).await?;
                        let (tx, rx) = mpsc::channel(1);
                        let client_id = self.client_id.clone();
                        let response_tx = self.response_tx.clone();
                        tokio::spawn(async move {
                            if let Err(err) =
                                exactly_once_server(client_id, packet_identifier, rx, response_tx)
                                    .await
                            {
                                error!(cause = ?err, "QoS 2 protocol error: {}", err);
                            }
                        });
                        self.server_flows.insert(packet_identifier, tx.clone());

                        Ok(Some(ControlPacket::PubRec(PublishResponse {
                            packet_identifier,
                            reason_code: ReasonCode::Success,
                            properties: Default::default(),
                        })))
                    } else {
                        Ok(Some(ControlPacket::Disconnect(Disconnect {
                            reason_code: ReasonCode::ReceiveMaximumExceeded,
                            properties: Default::default(),
                        })))
                    }
                } else {
                    Err(anyhow!("undefined packet_identifier"))
                }
            }
        }
    }

    #[instrument(skip(self), err)]
    async fn puback(&self, msg: PublishResponse) -> Result<Option<ControlPacket>> {
        trace!("{:?}", msg);
        Ok(None)
    }

    #[instrument(skip(self), err)]
    async fn pubrel(&self, msg: PublishResponse) -> Result<Option<ControlPacket>> {
        if let Some(tx) = self.server_flows.get(&msg.packet_identifier) {
            tx.clone()
                .send(PublishEvent::PubRel(msg))
                .await
                .context("PUBREL event")
                .map_err(Error::msg)?;
            Ok(None)
        } else {
            Err(anyhow!(
                "protocol flow error: not found flow for packet identifier {}",
                msg.packet_identifier
            ))
        }
    }

    #[instrument(skip(self), err)]
    async fn pubrec(&self, msg: PublishResponse) -> Result<Option<ControlPacket>> {
        if let Some(tx) = self.client_flows.get(&msg.packet_identifier) {
            tx.clone()
                .send(PublishEvent::PubRec(msg))
                .await
                .context("PUBREC event")
                .map_err(Error::msg)?;
            Ok(None)
        } else {
            Err(anyhow!(
                "protocol flow error: not found flow for packet identifier {}",
                msg.packet_identifier
            ))
        }
    }

    #[instrument(skip(self), err)]
    async fn pubcomp(&self, msg: PublishResponse) -> Result<Option<ControlPacket>> {
        if let Some(tx) = self.client_flows.get(&msg.packet_identifier) {
            tx.clone()
                .send(PublishEvent::PubComp(msg))
                .await
                .context("PUBCOMP event")
                .map_err(Error::msg)?;
            Ok(None)
        } else {
            Err(anyhow!(
                "protocol flow error: not found flow for packet identifier {}",
                msg.packet_identifier
            ))
        }
    }

    #[instrument(skip(self, msg), err)]
    async fn subscribe(&mut self, msg: Subscribe) -> Result<Option<ControlPacket>> {
        debug!("subscribe topic filters: {:?}", msg.topic_filters);
        let mut reason_codes = Vec::new();
        for (topic_filter, options) in msg.topic_filters {
            // add a subscription record in sled db
            self.context.store_subscription(
                self.client_id.as_str(),
                topic_filter.as_ref(),
                options.borrow(),
            )?;

            let topic_filter = std::str::from_utf8(&topic_filter[..])?;
            match self
                .context
                .subscribe(
                    self.client_id.as_str(),
                    topic_filter,
                    options.borrow(),
                    self.session_event_tx.borrow(),
                )
                .await
            {
                Ok((reason_code, unsubscribe)) => {
                    let (_, unsubscribes) = self
                        .topic_filters
                        .entry(topic_filter.to_owned())
                        .or_insert((options, Default::default()));
                    unsubscribes.extend(unsubscribe);
                    reason_codes.push(reason_code);
                }
                Err(err) => {
                    warn!(cause = ?err, "subscribe error: ");
                    reason_codes.push(ReasonCode::TopicFilterInvalid);
                }
            }
        }

        let suback = ControlPacket::SubAck(SubAck {
            packet_identifier: msg.packet_identifier,
            properties: Default::default(),
            reason_codes,
        });
        Ok(Some(suback))
        //self.response_tx.send(suback).await.map_err(Error::msg)
    }

    #[instrument(skip(self), err)]
    async fn suback(&self, msg: SubAck) -> Result<Option<ControlPacket>> {
        trace!("{:?}", msg);
        Ok(None)
    }

    #[instrument(skip(self, msg), err)]
    async fn unsubscribe(&mut self, msg: UnSubscribe) -> Result<Option<ControlPacket>> {
        debug!("topic filters: {:?}", msg.topic_filters);
        let mut reason_codes = Vec::new();
        for topic_filter in msg.topic_filters {
            reason_codes.push(match std::str::from_utf8(&topic_filter[..]) {
                Ok(topic_filter) => {
                    if let Some((_, removed)) = self.topic_filters.remove(topic_filter) {
                        debug!("stop {} subscriptions", removed.len())
                    };
                    self.context
                        .remove_subscription(self.client_id.as_str(), topic_filter.as_ref())?;
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
        Ok(Some(unsuback))
        //self.response_tx.send(unsuback).await.map_err(Error::msg)
        // trace!("send {:?}", suback);
    }

    #[instrument(skip(self), err)]
    async fn unsuback(&self, msg: SubAck) -> Result<Option<ControlPacket>> {
        trace!("{:?}", msg);
        Ok(None)
    }

    #[instrument(skip(self, msg))]
    async fn disconnect(&mut self, msg: Disconnect) -> Result<Option<ControlPacket>> {
        match (
            self.session_expire_interval,
            msg.properties.session_expire_interval,
        ) {
            (Some(0), Some(_)) => {
                return Ok(Some(ControlPacket::Disconnect(Disconnect {
                    reason_code: ReasonCode::ProtocolError,
                    properties: Default::default(),
                })))
            }
            (_, Some(session_expire_interval)) => {
                self.session_expire_interval = Some(session_expire_interval)
            }
            _ => {}
        };

        if msg.reason_code == ReasonCode::Success {
            // The Will Message MUST be removed from the stored Session State in the Server once
            // it has been published or the Server has received a DISCONNECT packet with a Reason Code
            // of 0x00 (Normal disconnection) from the Client
            self.will.take();
        }
        Ok(None)
    }

    pub async fn close_immediately(&mut self) {
        self.session_expire_interval = None;
        self.close().await
    }

    #[instrument(skip(self), fields(peer = field::display(& self.peer),
    client_id = field::debug(& self.client_id)))]
    pub async fn close(&mut self) {
        self.server_flows.clear();
        self.client_flows.clear();
        self.topic_filters.clear(); // stop active subscriptions

        let session_expire_interval = self.session_expire_interval.unwrap_or(0);
        let session_expire_interval = if let Some(will) = self.will.take() {
            self.send_will(session_expire_interval, will).await
        } else {
            session_expire_interval
        };
        if session_expire_interval > 0 {
            sleep(Duration::from_secs(session_expire_interval as u64)).await;
        };
        match self.context.get(self.client_id.as_str()).ok().flatten() {
            Some(SessionState { unique_id, .. }) if self.unique_id == unique_id => {
                self.context
                    .remove(self.client_id.as_str())
                    .unwrap_or_else(|error| {
                        warn!(cause = ?error, "session context remove error");
                    });
                let size = self
                    .context
                    .clear_subscriptions(self.client_id.as_str())
                    .unwrap_or_else(|error| {
                        warn!(cause = ?error, "session clear subscription error");
                        0
                    });
                debug!("session unsubscribed from {} subscriptions", size);
                info!("session closed");
            }
            _ => {
                debug!(
                    "Skip session remove. Session {} reconnected",
                    self.client_id
                );
            }
        };
    }

    async fn send_will(&mut self, session_expire_interval: u32, will: Will) -> u32 {
        trace!(
            "will_delay_interval: {} sec",
            will.properties.will_delay_interval
        );
        let delay = min(will.properties.will_delay_interval, session_expire_interval);
        if delay > 0 {
            sleep(Duration::from_secs(delay as u64)).await;
        }
        match self.context.get(self.client_id.as_str()).ok().flatten() {
            Some(SessionState { unique_id, .. }) if self.unique_id == unique_id => {
                debug!("send will: {:?}", will);
                let will = Publish {
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
                };
                if let Err(error) = self.context.publish(will).await {
                    warn!(cause = ?error, "will publish failure");
                }
            }
            _ => {
                debug!("Skip will. Session {} reconnected", self.client_id);
            }
        }
        session_expire_interval - delay
    }
}

impl Drop for Session {
    #[instrument(skip(self), fields(peer = field::display(& self.peer),
    client_id = field::debug(& self.client_id)))]
    fn drop(&mut self) {
        info!("drop session");
        metric::ACTIVE_SESSIONS.with_label_values(&[]).dec();
    }
}
