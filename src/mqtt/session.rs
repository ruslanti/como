use std::cmp::min;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Error, Result};
use tokio::stream::{StreamExt, StreamMap};
use tokio::sync::broadcast::RecvError::Lagged;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, field, instrument, trace, warn};

use crate::mqtt::proto::property::PubResProperties;
use crate::mqtt::proto::types::{
    ControlPacket, Disconnect, PacketType, PubResp, Publish, QoS, ReasonCode, SubAck, SubOption,
    Subscribe, UnSubscribe,
};
use crate::mqtt::topic::{Message, SubscribeTopic, Topic};
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
    PubAck(PubResp),
    PubRec(PubResp),
    PubRel(PubResp),
    PubComp(PubResp),
}

#[derive(Debug)]
pub(crate) struct Session {
    id: String,
    rx: Receiver<SessionEvent>,
    tx: Sender<ControlPacket>,
    config: Arc<Settings>,
    server_flows: HashMap<u16, Sender<PublishEvent>>,
    client_flows: HashMap<u16, Sender<PublishEvent>>,
    root_topic: Arc<RwLock<Topic>>,
    packet_identifier_seq: Arc<AtomicU16>,
    subscriptions: StreamMap<(String, SubOption), SubscribeTopic>,
}

#[instrument(skip(rx, tx), err)]
async fn exactly_once_client(
    session: String,
    packet_identifier: u16,
    mut rx: Receiver<PublishEvent>,
    mut tx: Sender<ControlPacket>,
) -> Result<()> {
    Ok(())
}

async fn publish_topic(root: Arc<RwLock<Topic>>, msg: Publish) -> Result<()> {
    let mut root = root.write().await;
    let topic = std::str::from_utf8(&msg.topic_name[..])?;
    let channel = root.publish_topic(topic);
    let message = Message {
        retain: msg.retain,
        qos: msg.qos,
        topic_name: msg.topic_name,
        content_type: msg.properties.content_type,
        data: msg.payload,
    };
    channel
        .send(message)
        .map_err(|e| anyhow!("{:?}", e))
        .map(|size| trace!("publish topic  size {}", size))
}

#[instrument(skip(rx, tx, root), err)]
async fn exactly_once_server(
    session: String,
    packet_identifier: u16,
    root: Arc<RwLock<Topic>>,
    mut rx: Receiver<PublishEvent>,
    mut tx: Sender<ControlPacket>,
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
                Some(event) => {
                    return Err(anyhow!("{} unknown event received: {:?}", session, event))
                }
                None => return Err(anyhow!("{} channel closed", session)),
            }
        } else {
            return Err(anyhow!("{} unexpected publish event {:?}", session, event));
        }
    }
    Ok(())
}

impl Session {
    pub fn new(
        id: &str,
        rx: Receiver<SessionEvent>,
        tx: Sender<ControlPacket>,
        config: Arc<Settings>,
        root_topic: Arc<RwLock<Topic>>,
    ) -> Self {
        Self {
            id: id.to_string(),
            rx,
            tx,
            config,
            server_flows: Default::default(),
            client_flows: Default::default(),
            root_topic,
            packet_identifier_seq: Arc::new(AtomicU16::new(1)),
            subscriptions: StreamMap::new(),
        }
    }

    #[instrument(skip(self), fields(identifier = field::display(& self.id)), err)]
    pub(crate) async fn session(&mut self) -> Result<()> {
        trace!("start");
        loop {
            tokio::select! {
                            Some(msg) = self.rx.next() => {
                                trace!("command: {:?}", msg);
                                match msg {
                                    SessionEvent::Publish(p) => self.publish(p).await?,
                                    SessionEvent::PubAck(p) => self.puback(p).await?,
                                    SessionEvent::PubRec(_p) => unimplemented!(),
                                    SessionEvent::PubRel(p) => self.pubrel(p).await?,
                                    SessionEvent::PubComp(_p) => unimplemented!(),
                                    SessionEvent::Subscribe(s) => self.subscribe(s).await?,
                                    SessionEvent::SubAck(_s) => unimplemented!(),
                                    SessionEvent::UnSubscribe(_s) => unimplemented!(),
                                    SessionEvent::UnSubAck(_s) => unimplemented!(),
                                    SessionEvent::Disconnect(d) => self.disconnect(d).await?,
                                }
                            },
                            Some(((topic, option), message)) = self.subscriptions.next() => {
                            match message {
                                Ok(msg) => {
                                    trace!("topic: {:?}, message: {:?}", topic, msg);
                                    let id = self.id.clone();
                                    let resp_tx = self.tx.clone();
                                    //let topic_name = topic.to_string();
                                    let packet_identifier = self.packet_identifier_seq.clone();
            /*                        tokio::spawn(async move {
                                        if let Err(err) = self.publish_client(
                                                    msg,
                                                    option,
                                                    packet_identifier,
                                                    resp_tx,
                                                ).await
                                            {
                                                error!(cause = ?err, "Subscribe flow error: {}", err);
                                            }
                                    });*/
                                },
                                Err(Lagged(lag)) => {
                                    warn!("lagged: {}", lag);
                                },
                                Err(err) => {
                                    error!(cause = ?err, "topic error: ");
                                    break;
                                }
                            }
                            },
                            else => break,
                        }
        }
        trace!("end");
        Ok(())
    }

    #[instrument(skip(self, msg), err)]
    async fn publish(&mut self, msg: Publish) -> Result<()> {
        match msg.qos {
            QoS::AtMostOnce => publish_topic(self.root_topic.clone(), msg).await?,
            QoS::AtLeastOnce => {
                if let Some(packet_identifier) = msg.packet_identifier {
                    publish_topic(self.root_topic.clone(), msg).await?;
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
                    return Err(anyhow!("undefined packet_identifier"));
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
                        let session = self.id.clone();
                        let (mut tx, rx) = mpsc::channel(1);
                        let root = self.root_topic.clone();
                        let resp_tx = self.tx.clone();
                        tokio::spawn(async move {
                            if let Err(err) =
                                exactly_once_server(session, packet_identifier, root, rx, resp_tx)
                                    .await
                            {
                                error!(cause = ?err, "QoS 2 protocol error: {}", err);
                            }
                        });
                        self.server_flows.insert(packet_identifier, tx.clone());
                        tx.send(PublishEvent::Publish(msg)).await?
                    }
                } else {
                    return Err(anyhow!("undefined packet_identifier"));
                }
            }
        }
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
    async fn puback(&self, msg: PubResp) -> Result<()> {
        trace!("{:?}", msg);
        Ok(())
    }

    #[instrument(skip(self, msg), err)]
    async fn subscribe(&mut self, msg: Subscribe) -> Result<()> {
        debug!("subscribe topics: {:?}", msg.topic_filters);
        let root = self.root_topic.read().await;
        //let mut subscriptions = self.subscriptions;
        let mut reason_codes = Vec::new();
        for (topic_name, topic_option) in msg.topic_filters {
            reason_codes.push(match std::str::from_utf8(&topic_name[..]) {
                Ok(topic) => {
                    let channels = root.subscribe_topic(topic);
                    for channel in channels {
                        self.subscriptions
                            .insert((topic.to_string(), topic_option.clone()), channel);
                    }
                    ReasonCode::Success
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

    #[instrument(skip(tx), err)]
    async fn publish_client(
        &mut self,
        msg: Message,
        option: SubOption,
        packet_identifier: Arc<AtomicU16>,
        mut tx: Sender<ControlPacket>,
    ) -> Result<()> {
        debug!("new subscribe spawn start");

        /* loop {
            match channel.recv().await {
                Ok(msg) => {
                    debug!("received: {:?}", msg);
                    let qos = min(msg.qos, option.qos);
                    let packet_identifier = if QoS::AtMostOnce == qos {
                        None
                    } else {
                        Some(packet_identifier.fetch_add(1, Ordering::Relaxed))
                    };
                    let publish = ControlPacket::Publish(Publish {
                        dup: false,
                        qos,
                        retain: msg.retain,
                        topic_name: msg.topic_name,
                        packet_identifier,
                        properties: Default::default(),
                        payload: msg.data,
                    });
                    tx.send(publish).await.map_err(Error::msg)?;

                    if QoS::ExactlyOnce == qos {
                        let (mut tx, rx) = mpsc::channel(1);
                        let packet_identifier = packet_identifier.unwrap();
                        self.server_flows.insert(packet_identifier, tx.clone());
                        let resp_tx = self.tx.clone();
                        let session = self.id.clone();
                        tokio::spawn(async move {
                            if let Err(err) =
                                exactly_once_client(session, packet_identifier, rx, resp_tx).await
                            {
                                error!(cause = ?err, "QoS 2 protocol error: {}", err);
                            }
                        });
                    }
                }
                Err(Lagged(lag)) => {
                    warn!("lagged: {}", lag);
                }
                Err(err) => {
                    error!(cause = ?err, "topic error: ");
                    break;
                }
            }
        }*/
        debug!("new subscribe spawn stop");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn disconnect(&self, msg: Disconnect) -> Result<()> {
        Ok(())
    }
}
