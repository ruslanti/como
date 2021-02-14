use std::cmp::min;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail, Error, Result};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tokio::time::Duration;
use tracing::{debug, error, field, info, instrument, trace, warn};

use crate::mqtt::proto::encoder::EncodedSize;
use crate::mqtt::proto::property::{ConnectProperties, PubResProperties, PublishProperties};
use crate::mqtt::proto::types::{
    ControlPacket, Disconnect, PacketType, PubResp, Publish, QoS, ReasonCode, SubAck, Subscribe,
    SubscriptionOptions, UnSubscribe, Will,
};

use crate::mqtt::topic::{TopicMessage, Topics};
use crate::settings::Settings;
use byteorder::{BigEndian, ReadBytesExt};
use sled::{Event, Subscriber};

pub(crate) type ConnectionContextState =
    (Sender<ControlPacket>, ConnectProperties, Option<Will>, bool);
type ConnectionContext = Option<ConnectionContextState>;

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum SessionEvent {
    Publish(Publish),
    PubAck(PubResp),
    PubRec(PubResp),
    PubRel(PubResp),
    PubComp(PubResp),
    Subscribe(Subscribe),
    SubAck(SubAck),
    UnSubscribe(UnSubscribe),
    UnSubAck(SubAck),
    Disconnect(Disconnect),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum PublishEvent {
    Publish(Publish),
    PubRec(PubResp),
    PubRel(PubResp),
    PubComp(PubResp),
}

#[derive(Debug)]
pub(crate) struct Session {
    id: String,
    created: Instant,
    expire_interval: Duration,
    connection: ConnectionContext,
    config: Arc<Settings>,
    server_flows: HashMap<u16, Sender<PublishEvent>>,
    client_flows: HashMap<u16, Sender<PublishEvent>>,
    topics: Arc<Topics>,
    packet_identifier_seq: Arc<AtomicU16>,
    topic_filters: HashMap<(String, SubscriptionOptions), Vec<oneshot::Sender<()>>>,
}

async fn publish_topic(root: Arc<Topics>, msg: Publish) -> Result<()> {
    let topic = std::str::from_utf8(&msg.topic_name[..])?;

    let message = TopicMessage {
        ts: Instant::now(),
        retain: msg.retain,
        qos: msg.qos,
        properties: msg.properties,
        payload: msg.payload,
    };

    root.publish(topic, message).await
}

#[instrument(skip(rx, connection_reply_tx), err)]
async fn exactly_once_client(
    session: String,
    packet_identifier: u16,
    mut rx: Receiver<PublishEvent>,
    connection_reply_tx: Sender<ControlPacket>,
) -> Result<()> {
    if let Some(event) = rx.recv().await {
        if let PublishEvent::PubRec(_msg) = event {
            let rel = ControlPacket::PubRel(PubResp {
                packet_type: PacketType::PUBREL,
                packet_identifier,
                reason_code: ReasonCode::Success,
                properties: Default::default(),
            });
            connection_reply_tx.send(rel).await?;

            match rx.recv().await {
                Some(PublishEvent::PubComp(comp)) => {
                    trace!("{:?}", comp);
                }
                Some(event) => bail!("{} unknown event received: {:?}", session, event),
                None => bail!("{} channel closed", session),
            }
        } else {
            bail!("{} unexpected pubrec event {:?}", session, event);
        }
    }
    Ok(())
}

#[instrument(skip(rx, connection_reply_tx, root), err)]
async fn exactly_once_server(
    session: String,
    packet_identifier: u16,
    root: Arc<Topics>,
    mut rx: Receiver<PublishEvent>,
    connection_reply_tx: Sender<ControlPacket>,
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
            connection_reply_tx.send(rec).await?;

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
                    connection_reply_tx.send(comp).await?;
                }
                Some(event) => bail!("{} unknown event received: {:?}", session, event),
                None => bail!("{} channel closed", session),
            }
        } else {
            bail!("{} unexpected publish event {:?}", session, event);
        }
    }
    Ok(())
}

impl Session {
    #[instrument(skip(id, config, topic_manager),
    fields(identifier = field::display(& id)))]
    pub fn new(id: &str, config: Arc<Settings>, topic_manager: Arc<Topics>) -> Self {
        info!("new session");
        Self {
            id: id.to_owned(),
            created: Instant::now(),
            expire_interval: Duration::from_millis(1),
            connection: None,
            config,
            server_flows: HashMap::new(),
            client_flows: HashMap::new(),
            topics: topic_manager,
            packet_identifier_seq: Arc::new(AtomicU16::new(1)),
            topic_filters: HashMap::new(),
        }
    }

    fn clear(&mut self) {
        self.server_flows.clear();
        self.client_flows.clear();
        self.topic_filters.clear();
    }

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
                                    Ok(Some((connection_reply_tx, prop, will, clean_start))) => {
                                        debug!("new connection context");
                                        if let Some(ex) = prop.session_expire_interval {
                                            self.expire_interval = Duration::from_secs(ex as u64);
                                        }
                                        self.clear();
                                        self.connection = Some((connection_reply_tx, prop, will, clean_start));
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

    fn filtered(&self, topic: &str) -> Vec<(String, SubscriptionOptions)> {
        self.topic_filters
            .keys()
            .filter(|(filter, _)| Topics::match_filter(topic, filter.as_str()))
            .map(|(f, o)| (f.to_owned(), o.to_owned()))
            .collect()
    }

    #[instrument(skip(self, msg), fields(identifier = field::display(& self.id)), err)]
    async fn publish_client(
        &mut self,
        option: SubscriptionOptions,
        msg: TopicMessage,
    ) -> Result<()> {
        match &self.connection {
            Some((connection_reply_tx, properties, _, _)) => {
                let packet_identifier = self.packet_identifier_seq.clone();
                let qos = min(msg.qos, option.qos);
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
                    properties: msg.properties,
                    payload: msg.payload,
                });

                if let Some(maximum_packet_size) = properties.maximum_packet_size {
                    if publish.encoded_size() > maximum_packet_size as usize {
                        warn!("exceed maximum_packet_size: {:?}", publish);
                        return Ok(());
                    }
                }

                connection_reply_tx
                    .send(publish)
                    .await
                    .map_err(Error::msg)?;

                if QoS::ExactlyOnce == qos {
                    let (tx, rx) = mpsc::channel(1);
                    let packet_identifier = packet_identifier.unwrap();
                    self.client_flows.insert(packet_identifier, tx);
                    let connection_reply_tx = connection_reply_tx.clone();
                    let session = self.id.clone();
                    tokio::spawn(async move {
                        if let Err(err) =
                            exactly_once_client(session, packet_identifier, rx, connection_reply_tx)
                                .await
                        {
                            error!(cause = ?err, "QoS 2 protocol error: {}", err);
                        }
                    });
                };
                Ok(())
            }
            None => bail!("publish client event in not connected session"),
        }
    }

    #[instrument(skip(self, msg), err)]
    async fn publish(&mut self, msg: Publish) -> Result<()> {
        match &self.connection {
            Some((connection_reply_tx, _, _, _)) => {
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
                            trace!("send {:?}", ack);
                            connection_reply_tx.send(ack).await?
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
                                connection_reply_tx.send(disconnect).await?;
                            } else if self.server_flows.len()
                                < self.config.connection.receive_maximum.unwrap() as usize
                            {
                                let session = self.id.clone();
                                let (tx, rx) = mpsc::channel(1);
                                let root = self.topics.clone();
                                let connection_reply_tx = connection_reply_tx.clone();
                                tokio::spawn(async move {
                                    if let Err(err) = exactly_once_server(
                                        session,
                                        packet_identifier,
                                        root,
                                        rx,
                                        connection_reply_tx,
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
                                connection_reply_tx.send(disconnect).await?;
                            }
                        } else {
                            bail!("undefined packet_identifier");
                        }
                    }
                }
                Ok(())
            }
            None => bail!("publish event in not connected session"),
        }
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

    #[instrument(skip(subscriber))]
    pub(crate) async fn subscription(
        session: String,
        topic_name: &str,
        mut subscriber: Subscriber,
    ) -> Result<()> {
        //   tokio::pin!(stream);
        trace!("start");
        while let Some(event) = (&mut subscriber).await {
            match event {
                Event::Insert { key, value } => match key.as_ref().read_u64::<BigEndian>() {
                    Ok(id) => {
                        debug!("id: {}, value: {:#x?}", id, value);
                    }
                    Err(_) => warn!("invalid log id: {:#x?}", key),
                },
                Event::Remove { .. } => {}
            }
        }

        trace!("end");
        Ok(())
    }

    #[instrument(skip(self, msg), err)]
    async fn subscribe(&mut self, msg: Subscribe) -> Result<()> {
        match &self.connection {
            Some((connection_reply_tx, _, _, _)) => {
                debug!("subscribe topic filters: {:?}", msg.topic_filters);
                let mut reason_codes = Vec::new();
                for (topic_filter, subscription_options) in msg.topic_filters {
                    reason_codes.push(match std::str::from_utf8(&topic_filter[..]) {
                        Ok(topic_filter) => {
                            let subscriptions = self
                                .topic_filters
                                .entry((topic_filter.to_owned(), subscription_options.to_owned()))
                                .or_insert(vec![]);
                            let channels = self.topics.subscribe(topic_filter).await?;
                            trace!("subscribe returns {} subscriptions", channels.len());
                            for (topic, subscriber) in channels {
                                let (unsubscribe_tx, unsubscribe_rx) = oneshot::channel();
                                subscriptions.push(unsubscribe_tx);
                                let session = self.id.to_owned();
                                tokio::spawn(async move {
                                    tokio::select! {
                                        Err(err) = Self::subscription(session, topic.as_str(),
                                        subscriber) => {
                                            warn!(cause = ?err, "subscription error");
                                        },
                                        _ = unsubscribe_rx => {
                                            debug!("unsubscribe");
                                        }
                                    }
                                });
                            }

                            match subscription_options.qos {
                                QoS::AtMostOnce => ReasonCode::Success,
                                QoS::AtLeastOnce => ReasonCode::GrantedQoS1,
                                QoS::ExactlyOnce => ReasonCode::GrantedQoS2,
                            }
                        }
                        Err(err) => {
                            debug!(cause = ?err, "subscribe error: ");
                            ReasonCode::TopicFilterInvalid
                        }
                    })
                }

                let suback = ControlPacket::SubAck(SubAck {
                    packet_identifier: msg.packet_identifier,
                    properties: Default::default(),
                    reason_codes,
                });
                connection_reply_tx.send(suback).await.map_err(Error::msg)
            }
            None => bail!("subscribe event in not connected session"),
        }
        // trace!("send {:?}", suback);
    }

    #[instrument(skip(self), err)]
    async fn suback(&self, msg: SubAck) -> Result<()> {
        trace!("{:?}", msg);
        Ok(())
    }

    #[instrument(skip(self, msg), err)]
    async fn unsubscribe(&mut self, msg: UnSubscribe) -> Result<()> {
        match &self.connection {
            Some((connection_reply_tx, _, _, _)) => {
                debug!("topic filters: {:?}", msg.topic_filters);
                let mut reason_codes = Vec::new();
                for topic_filter in msg.topic_filters {
                    reason_codes.push(match std::str::from_utf8(&topic_filter[..]) {
                        Ok(topic_filter) => {
                            let keys: Vec<(String, SubscriptionOptions)> = self
                                .topic_filters
                                .keys()
                                .filter_map(|(k, opt)| {
                                    if k == topic_filter {
                                        Some((k.to_owned(), opt.to_owned()))
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            for key in keys {
                                if let Some(channel) = self.topic_filters.remove(&key) {
                                    debug!(
                                        "unsubscribe topic_filter: {}, channel: {:?}",
                                        topic_filter, channel
                                    );
                                }
                            }
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
                connection_reply_tx.send(unsuback).await.map_err(Error::msg)
                // trace!("send {:?}", suback);
            }
            None => bail!("unsubscribe event in not connected session"),
        }
    }

    #[instrument(skip(self), err)]
    async fn unsuback(&self, msg: SubAck) -> Result<()> {
        trace!("{:?}", msg);
        Ok(())
    }

    #[instrument(skip(self, msg))]
    async fn disconnect(&self, msg: Disconnect) -> Result<()> {
        match &self.connection {
            Some((_, _session_expire_interval, will, _)) => {
                if msg.reason_code != ReasonCode::Success {
                    // publish will
                    if let Some(will) = will {
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
                                    payload_format_indicator: will
                                        .properties
                                        .payload_format_indicator,
                                    message_expire_interval: will
                                        .properties
                                        .message_expire_interval,
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
            None => bail!("disconnect event for not connected session"),
        }
    }
}

impl Drop for Session {
    #[instrument(skip(self), fields(identifier = field::display(& self.id)))]
    fn drop(&mut self) {
        info!("session closed");
    }
}
