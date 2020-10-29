use std::borrow::Borrow;
use std::cmp::min;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail, Error, Result};
use tokio::stream::StreamExt;
use tokio::sync::broadcast::error::RecvError::Lagged;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{debug, error, field, info, instrument, trace, warn};

use crate::mqtt::proto::property::{PubResProperties, PublishProperties};
use crate::mqtt::proto::types::{
    ControlPacket, Disconnect, PacketType, PubResp, Publish, QoS, ReasonCode, SubAck, Subscribe,
    SubscriptionOptions, UnSubscribe, Will,
};
use crate::mqtt::subscription::{SessionSender, Subscription};
use crate::mqtt::topic::{Message, TopicManager};
use crate::settings::Settings;

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
    rx: Receiver<SessionEvent>,
    tx: Sender<ControlPacket>,
    config: Arc<Settings>,
    server_flows: HashMap<u16, Sender<PublishEvent>>,
    client_flows: HashMap<u16, Sender<PublishEvent>>,
    topic_manager: Arc<RwLock<TopicManager>>,
    packet_identifier_seq: Arc<AtomicU16>,
    topic_filters: HashMap<(String, SubscriptionOptions), Vec<oneshot::Sender<()>>>,
    will: Option<Will>,
}

async fn publish_topic(root: Arc<RwLock<TopicManager>>, msg: Publish) -> Result<()> {
    let mut topic_manager = root.write().await;
    let topic = std::str::from_utf8(&msg.topic_name[..])?;

    let new_topic_channel = topic_manager.new_topic_channel();
    let channel = topic_manager.publish(topic, |name, channel, retained| {
        let new_topic = (name, channel, retained);
        if let Err(err) = new_topic_channel.send(new_topic) {
            error!(cause = ?err, "publish topic error");
        };
        ()
    })?;

    let message = Message {
        ts: Instant::now(),
        retain: msg.retain,
        qos: msg.qos,
        topic_name: msg.topic_name,
        properties: msg.properties,
        payload: msg.payload,
    };

    channel
        .send(message)
        .map_err(|e| anyhow!("{:?}", e))
        .map(|size| trace!("publish topic channel send {} message", size))
}

#[instrument(skip(rx, tx), err)]
async fn exactly_once_client(
    session: String,
    packet_identifier: u16,
    mut rx: Receiver<PublishEvent>,
    tx: Sender<ControlPacket>,
) -> Result<()> {
    if let Some(event) = rx.recv().await {
        if let PublishEvent::PubRec(_msg) = event {
            let rel = ControlPacket::PubRel(PubResp {
                packet_type: PacketType::PUBREL,
                packet_identifier,
                reason_code: ReasonCode::Success,
                properties: Default::default(),
            });
            tx.send(rel).await?;

            match rx.recv().await {
                Some(PublishEvent::PubComp(comp)) => {
                    trace!("{:?}", comp);
                    ()
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

#[instrument(skip(rx, tx, root), err)]
async fn exactly_once_server(
    session: String,
    packet_identifier: u16,
    root: Arc<RwLock<TopicManager>>,
    mut rx: Receiver<PublishEvent>,
    tx: Sender<ControlPacket>,
) -> Result<()> {
    if let Some(event) = rx.recv().await {
        if let PublishEvent::Publish(msg) = event {
            publish_topic(root, msg).await?;
            let rec = ControlPacket::PubRec(PubResp {
                packet_type: PacketType::PUBREC,
                packet_identifier,
                reason_code: ReasonCode::Success,
                properties: Default::default(),
            });
            tx.send(rec).await?;

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
                    tx.send(comp).await?;
                    ()
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

/*async fn subscribe_topic(
    mut channel: SubscribeChannel,
    retain_tx: Sender<(String, SubscribeOptions, Message)>,
) -> Result<()> {
    loop {
        retain_tx.send(channel.recv().await?).map_err(Error::msg())
    }
    Ok(())
}*/

impl Session {
    #[instrument(skip(id, rx, tx, config, topic_manager), fields(identifier = field::display(&
    id)))]
    pub fn new(
        id: &str,
        rx: Receiver<SessionEvent>,
        tx: Sender<ControlPacket>,
        config: Arc<Settings>,
        topic_manager: Arc<RwLock<TopicManager>>,
        will: Option<Will>,
    ) -> Self {
        info!("new session");
        Self {
            id: id.to_owned(),
            created: Instant::now(),
            rx,
            tx,
            config,
            server_flows: HashMap::new(),
            client_flows: HashMap::new(),
            topic_manager,
            packet_identifier_seq: Arc::new(AtomicU16::new(1)),
            topic_filters: HashMap::new(),
            will,
        }
    }

    #[instrument(skip(self), fields(identifier = field::display(& self.id)), err)]
    pub(crate) async fn session(&mut self) -> Result<()> {
        trace!("start");
        // subscribe to root topic for new topic creation
        let new_topic_stream = self
            .topic_manager
            .read()
            .await
            .subscribe_channel()
            .into_stream();
        tokio::pin!(new_topic_stream);
        let (tx, mut rx) = mpsc::channel(32);
        loop {
            tokio::select! {
                Some(msg) = self.rx.next() => {
                    trace!("command: {:?}", msg);
                    match msg {
                        SessionEvent::Publish(p) => self.publish(p).await?,
                        SessionEvent::PubAck(p) => self.puback(p).await?,
                        SessionEvent::PubRec(p) => self.pubrec(p).await?,
                        SessionEvent::PubRel(p) => self.pubrel(p).await?,
                        SessionEvent::PubComp(p) => self.pubcomp(p).await?,
                        SessionEvent::Subscribe(s) => self.subscribe(s, tx.clone()).await?,
                        SessionEvent::SubAck(s) => self.suback(s).await?,
                        SessionEvent::UnSubscribe(s) => self.unsubscribe(s).await?,
                        SessionEvent::UnSubAck(s) => self.unsuback(s).await?,
                        SessionEvent::Disconnect(d) => {
                            self.disconnect(d).await?;
                            //TODO handle session expire interval
                            break;
                        }
                    }
                },
                Some(event) = new_topic_stream.next() => {
                    match event {
                        Ok((topic, topic_tx, retained)) => {
                            trace!("new topic event {:?}", topic);
                            let filtered: Vec<(String, SubscriptionOptions)> = self
                                .topic_filters
                                .keys()
                                .filter(|(filter, _)| TopicManager::match_filter(topic.as_str(),
                                filter.as_str()))
                                .map(|(f, o)| (f.to_owned(), o.to_owned()))
                                .collect();
                            for (filter, option) in filtered {
                                let stream = topic_tx.subscribe().into_stream();
                                debug!("new topic: {} match topic filter {}", topic, filter);
                                let (unsubscribe_tx, unsubscribe_rx) = oneshot::channel();
                                let subscription = Subscription::new(
                                    self.id.to_owned(),
                                    option.to_owned(),
                                    tx.clone(),
                                );
                                self.topic_filters.entry((filter.to_owned(), option.to_owned())).and_modify(|v| v.push(unsubscribe_tx));

                                let topic_name = topic.to_owned();
                                tokio::spawn(async move {
                                   subscription.subscribe(topic_name, stream, unsubscribe_rx).await
                                });

                                if let Some(retained) = retained.borrow().clone() {
                                    if retained.retain || retained.ts > self.created {
                                        let ret_msg = (filter, option, retained);
                                        let tx = tx.to_owned();
                                        tokio::spawn(async move {
                                            if let Err(err) = tx.send(ret_msg).await {
                                                warn!(cause = ?err, "could not process retained message");
                                            }
                                        });
                                    }
                                }
                            }
                        },
                        Err(Lagged(lag)) => {
                            warn!("new topic event lagged: {}", lag);
                        },
                        Err(err) => {
                            warn!(cause = ?err, "new topic event error: ");
                        }
                    }
                }
                Some((topic_filter, option, message)) = rx.next() => {
                    trace!("publish topic: {}, message: {:?}", topic_filter, message);
                    self.publish_client(option, message).await?
                },
                else => break,
            }
        }
        trace!("stop");
        Ok(())
    }

    #[instrument(skip(self), fields(identifier = field::display(& self.id)), err)]
    async fn publish_client(&mut self, option: SubscriptionOptions, msg: Message) -> Result<()> {
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
            topic_name: msg.topic_name,
            packet_identifier,
            properties: msg.properties,
            payload: msg.payload,
        });
        self.tx.send(publish).await.map_err(Error::msg)?;

        if QoS::ExactlyOnce == qos {
            let (tx, rx) = mpsc::channel(1);
            let packet_identifier = packet_identifier.unwrap();
            self.client_flows.insert(packet_identifier, tx.clone());
            let resp_tx = self.tx.clone();
            let session = self.id.clone();
            tokio::spawn(async move {
                if let Err(err) = exactly_once_client(session, packet_identifier, rx, resp_tx).await
                {
                    error!(cause = ?err, "QoS 2 protocol error: {}", err);
                }
            });
        };
        Ok(())
    }

    #[instrument(skip(self, msg), err)]
    async fn publish(&mut self, msg: Publish) -> Result<()> {
        match msg.qos {
            QoS::AtMostOnce => publish_topic(self.topic_manager.clone(), msg).await?,
            QoS::AtLeastOnce => {
                if let Some(packet_identifier) = msg.packet_identifier {
                    publish_topic(self.topic_manager.clone(), msg).await?;
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
                    self.tx.send(ack).await?
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
                        self.tx.send(disconnect).await?;
                    } else {
                        if self.server_flows.len()
                            < self.config.connection.receive_maximum.unwrap() as usize
                        {
                            let session = self.id.clone();
                            let (tx, rx) = mpsc::channel(1);
                            let root = self.topic_manager.clone();
                            let resp_tx = self.tx.clone();
                            tokio::spawn(async move {
                                if let Err(err) = exactly_once_server(
                                    session,
                                    packet_identifier,
                                    root,
                                    rx,
                                    resp_tx,
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
                            self.tx.send(disconnect).await?;
                        }
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

    #[instrument(skip(self, msg, tx), err)]
    async fn subscribe(&mut self, msg: Subscribe, tx: SessionSender) -> Result<()> {
        debug!("subscribe topic filters: {:?}", msg.topic_filters);
        let root = self.topic_manager.read().await;
        let mut reason_codes = Vec::new();
        for (topic_filter, option) in msg.topic_filters {
            reason_codes.push(match std::str::from_utf8(&topic_filter[..]) {
                Ok(topic_filter) => {
                    let subscriptions = self
                        .topic_filters
                        .entry((topic_filter.to_owned(), option.to_owned()))
                        .or_insert(vec![]);
                    let channels = root.subscribe(topic_filter);
                    trace!("subscribe returns {} subscriptions", channels.len());
                    for (topic, channel, retained_msg) in channels {
                        let (unsubscribe_tx, unsubscribe_rx) = oneshot::channel();
                        let subscription =
                            Subscription::new(self.id.to_owned(), option.to_owned(), tx.clone());
                        subscriptions.push(unsubscribe_tx);

                        tokio::spawn(async move {
                            subscription
                                .subscribe(topic.to_owned(), channel.into_stream(), unsubscribe_rx)
                                .await
                        });

                        if let Some(retained) = retained_msg.borrow().clone() {
                            if retained.retain || retained.ts > self.created {
                                let ret_msg =
                                    (topic_filter.to_owned(), option.to_owned(), retained);
                                let tx = tx.to_owned();
                                tokio::spawn(async move {
                                    if let Err(err) = tx.send(ret_msg).await {
                                        warn!(cause = ?err, "could not process retained message");
                                    }
                                });
                            }
                        }
                    }
                    match option.qos {
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
        self.tx.send(suback).await.map_err(Error::msg)
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
        self.tx.send(unsuback).await.map_err(Error::msg)
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
                    self.topic_manager.clone(),
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

impl Drop for Session {
    #[instrument(skip(self), fields(identifier = field::display(& self.id)))]
    fn drop(&mut self) {
        info!("session closed");
    }
}
