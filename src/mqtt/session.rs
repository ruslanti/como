use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use futures::{future, Future};
use tokio::stream::StreamExt;
use tokio::sync::broadcast::RecvError::Lagged;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, RwLock};
use tokio::time::timeout;
use tracing::{debug, error, instrument, trace, warn};
use uuid::Uuid;

use crate::mqtt::proto::property::{
    ConnAckProperties, ConnectProperties, DisconnectProperties, PubResProperties,
    PublishProperties, SubAckProperties,
};
use crate::mqtt::proto::types::{
    Auth, ConnAck, Connect, ControlPacket, Disconnect, PacketType, PubResp, Publish, QoS,
    ReasonCode, SubAck, Subscribe, UnSubscribe, Will,
};
use crate::mqtt::topic::{Message, SubscribeTopic, Topic};
use crate::settings::{ConnectionSettings, Settings};

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
    flows: HashMap<u16, Sender<PublishEvent>>,
    root_topic: Arc<RwLock<Topic>>,
}

async fn subscribe_flow(mut channel: SubscribeTopic, mut tx: Sender<ControlPacket>) -> Result<()> {
    debug!("new subscribe spawn start  {:?}", channel);
    loop {
        match channel.recv().await {
            Ok(msg) => {
                debug!("received: {:?}", msg);
                let publish = ControlPacket::Publish(Publish {
                    dup: false,
                    qos: QoS::AtMostOnce,
                    retain: false,
                    topic_name: "".to_string().into(),
                    packet_identifier: None,
                    properties: Default::default(),
                    payload: Default::default(),
                });
                tx.send(publish)
                    .await
                    .map_err(|e| anyhow!("publish send error"))?
            }
            Err(Lagged(lag)) => {
                warn!("lagged: {}", lag);
            }
            Err(err) => {
                error!(cause = ?err, "topic error: ");
                break;
            }
        }
    }
    debug!("new subscribe spawn stop {:?}", channel);
    Ok(())
}

async fn publish_topic(root: Arc<RwLock<Topic>>, msg: Publish) -> Result<()> {
    let mut root = root.write().await;
    let topic = std::str::from_utf8(&msg.topic_name[..])?;
    let channel = root.publish(topic);
    let message = Message {
        retain: msg.retain,
        content_type: msg.properties.content_type,
        data: msg.payload,
    };
    channel
        .send(message)
        .map_err(|e| anyhow!("publish send error for topic {}", topic))
        .map(|size| trace!("publish topic {} size {}", topic, size))
}

async fn exactly_once_flow(
    packet_identifier: u16,
    root: Arc<RwLock<Topic>>,
    mut rx: Receiver<PublishEvent>,
    mut tx: Sender<ControlPacket>,
) -> Result<()> {
    trace!("start flow {}", packet_identifier);
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

            match timeout(Duration::from_secs(1), rx.recv()).await {
                Ok(Some(PublishEvent::PubRel(rel))) => {
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
                Ok(Some(event)) => return Err(anyhow!("unknown event received: {:?}", event)),
                Ok(None) => return Err(anyhow!("channel closed")),
                Err(elapsed) => return Err(anyhow!("pub rel receive timeout: {}", elapsed)),
            }
        } else {
            return Err(anyhow!("unexpected publish event {:?}", event));
        }
    }
    trace!("end flow {}", packet_identifier);
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
            flows: Default::default(),
            root_topic,
        }
    }

    #[instrument(skip(self))]
    pub(crate) async fn run(&mut self) -> Result<()> {
        trace!("start session {}", self.id);
        while let Some(msg) = self.rx.next().await {
            debug!("{:?}", msg);
            match msg {
                SessionEvent::Publish(p) => self.publish(p).await?,
                SessionEvent::PubAck(p) => unimplemented!(),
                SessionEvent::PubRec(p) => unimplemented!(),
                SessionEvent::PubRel(p) => self.pubrel(p).await?,
                SessionEvent::PubComp(p) => unimplemented!(),
                SessionEvent::Subscribe(s) => self.subscribe(s).await?,
                SessionEvent::SubAck(s) => unimplemented!(),
                SessionEvent::UnSubscribe(s) => unimplemented!(),
                SessionEvent::UnSubAck(s) => unimplemented!(),
                SessionEvent::Disconnect(d) => self.disconnect(d).await?,
            }
        }
        trace!("end session {}", self.id);
        Ok(())
    }

    #[instrument(skip(self))]
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
                    if self.flows.contains_key(&packet_identifier) && !msg.dup {
                        let disconnect = ControlPacket::Disconnect(Disconnect {
                            reason_code: ReasonCode::PacketIdentifierInUse,
                            properties: Default::default(),
                        });
                        self.tx.send(disconnect).await?;
                    } else {
                        let (mut tx, rx) = mpsc::channel(1);
                        let root = self.root_topic.clone();
                        let resp_tx = self.tx.clone();
                        tokio::spawn(async move {
                            if let Err(err) =
                                exactly_once_flow(packet_identifier, root, rx, resp_tx).await
                            {
                                error!(cause = ?err, "QoS 2 protocol error: {}", err);
                            }
                        });
                        self.flows.insert(packet_identifier, tx.clone());
                        tx.send(PublishEvent::Publish(msg)).await?
                    }
                } else {
                    return Err(anyhow!("undefined packet_identifier"));
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self))]
    async fn pubrel(&self, msg: PubResp) -> Result<()> {
        if let Some(tx) = self.flows.get(&msg.packet_identifier) {
            tx.clone()
                .send(PublishEvent::PubRel(msg))
                .await
                .map_err(|e| anyhow!("pubrel send error"))
        } else {
            Err(anyhow!(
                "protocol flow error: not found flow for packet identifier {}",
                msg.packet_identifier
            ))
        }
    }

    #[instrument(skip(self))]
    async fn subscribe(&mut self, subscribe: Subscribe) -> Result<()> {
        debug!("subscribe topics: {:?}", subscribe.topic_filters);

        let root = self.root_topic.read().await;
        let reason_codes = subscribe
            .topic_filters
            .iter()
            .map(
                |(topic_name, topic_option)| match std::str::from_utf8(&topic_name[..]) {
                    Ok(topic) => {
                        let channels = root.subscribe(topic);
                        for mut channel in channels {
                            let resp_tx = self.tx.clone();
                            tokio::spawn(async move {
                                if let Err(err) = subscribe_flow(channel, resp_tx).await {
                                    error!(cause = ?err, "Subscribe flow error: {}", err);
                                }
                            });
                        }
                        ReasonCode::Success
                    }
                    Err(err) => {
                        debug!(cause = ?err, "subscribe error: ");
                        ReasonCode::TopicFilterInvalid
                    }
                },
            )
            .collect();

        let suback = ControlPacket::SubAck(SubAck {
            packet_identifier: subscribe.packet_identifier,
            properties: Default::default(),
            reason_codes,
        });
        trace!("send {:?}", suback);
        Ok(())
    }

    #[instrument(skip(self))]
    async fn disconnect(&self, msg: Disconnect) -> Result<()> {
        Ok(())
    }
}
