use std::net::SocketAddr;

use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::time::timeout;
use tokio::time::Duration;
use tokio_util::codec::Framed;
use tracing::trace;

use crate::identifier::Sequence;
use crate::v5::property::{PropertiesBuilder, SubscribeProperties};
use crate::v5::types::{
    ConnAck, Connect, ControlPacket, Disconnect, MQTTCodec, MqttString, Publish, PublishResponse,
    QoS, ReasonCode, Retain, SubAck, Subscribe, SubscriptionOptions,
};

pub struct MqttClient {
    stream: Framed<TcpStream, MQTTCodec>,
    client_id: Option<MqttString>,
    keep_alive: u16,
    properties_builder: PropertiesBuilder,
    timeout: Duration,
    packet_identifier: Sequence,
}

pub struct ClientBuilder<'a> {
    address: &'a str,
    client_id: Option<MqttString>,
    keep_alive: Option<u16>,
    properties_builder: PropertiesBuilder,
}

impl<'a> MqttClient {
    pub fn builder(address: &'a str) -> ClientBuilder {
        ClientBuilder {
            address,
            client_id: None,
            keep_alive: None,
            properties_builder: PropertiesBuilder::default(),
        }
    }

    pub async fn connect(&mut self, clean_start: bool) -> Result<ConnAck> {
        let connect = Connect {
            clean_start_flag: clean_start,
            keep_alive: self.keep_alive,
            properties: self.properties_builder.to_owned().connect(),
            client_identifier: self.client_id.to_owned(),
            username: None,
            password: None,
            will: None,
        };
        trace!("send {:?}", connect);
        self.stream.send(ControlPacket::Connect(connect)).await?;
        self.recv().await.and_then(|packet| match packet {
            ControlPacket::ConnAck(ack) => Ok(ack),
            _ => Err(anyhow!("unexpected: {:?}", packet)),
        })
    }

    pub async fn recv(&mut self) -> Result<ControlPacket, Error> {
        timeout(self.timeout, self.stream.next())
            .await
            .map_err(Error::msg)
            .and_then(|r| r.ok_or_else(|| anyhow!("none message")))
            .and_then(|r| r)
    }

    pub async fn disconnect(&mut self) -> Result<Option<ControlPacket>> {
        self.disconnect_with_reason(ReasonCode::Success).await
    }

    pub async fn disconnect_with_reason(
        &mut self,
        reason_code: ReasonCode,
    ) -> Result<Option<ControlPacket>> {
        let disconnect = Disconnect {
            reason_code,
            properties: Default::default(),
        };

        trace!("send {:?}", disconnect);
        self.stream
            .send(ControlPacket::Disconnect(disconnect))
            .await?;
        // expected None on socket close
        self.stream.next().await.transpose()
    }

    pub async fn publish_most_once(
        &mut self,
        retain: bool,
        topic_name: String,
        payload: Vec<u8>,
    ) -> Result<()> {
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain,
            topic_name: Bytes::from(topic_name),
            packet_identifier: None,
            properties: Default::default(),
            payload: Bytes::from(payload),
        };
        trace!("send {:?}", publish);
        self.stream.send(ControlPacket::Publish(publish)).await
    }

    pub async fn publish_least_once(
        &mut self,
        retain: bool,
        topic_name: String,
        payload: Vec<u8>,
    ) -> Result<PublishResponse> {
        let packet_identifier = self.packet_identifier.next()?;
        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain,
            topic_name: Bytes::from(topic_name),
            packet_identifier: Some(packet_identifier.value()),
            properties: Default::default(),
            payload: Bytes::from(payload),
        };
        trace!("send {:?}", publish);
        self.stream.send(ControlPacket::Publish(publish)).await?;
        self.recv().await.and_then(|packet| match packet {
            ControlPacket::PubAck(ack) => Ok(ack),
            _ => Err(anyhow!("unexpected: {:?}", packet)),
        })
    }

    pub async fn subscribe(&mut self, qos: QoS, topic_filter: String) -> Result<SubAck> {
        let packet_identifier = self.packet_identifier.next()?;
        let subscribe = Subscribe {
            packet_identifier: packet_identifier.value(),
            properties: SubscribeProperties::default(),
            topic_filters: vec![(
                Bytes::from(topic_filter),
                SubscriptionOptions {
                    qos,
                    nl: false,
                    rap: false,
                    retain: Retain::SendAtTime,
                },
            )],
        };
        trace!("send {:?}", subscribe);
        self.stream
            .send(ControlPacket::Subscribe(subscribe))
            .await?;
        self.recv().await.and_then(|packet| match packet {
            ControlPacket::SubAck(ack) => Ok(ack),
            _ => Err(anyhow!("unexpected: {:?}", packet)),
        })
    }
}

impl ClientBuilder<'_> {
    pub fn client_id(mut self, value: &'static str) -> Self {
        self.client_id = Some(MqttString::from(value));
        self
    }

    pub fn keep_alive(mut self, value: u16) -> Self {
        self.keep_alive = Some(value);
        self
    }

    pub fn session_expire_interval(mut self, value: u32) -> Self {
        self.properties_builder = self
            .properties_builder
            .session_expire_interval(value)
            .unwrap();
        self
    }

    pub async fn build(self) -> Result<MqttClient> {
        let peer: SocketAddr = self.address.parse()?;
        let socket = if peer.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };

        let stream = socket.connect(peer).await?;
        let stream = Framed::new(stream, MQTTCodec::default());

        Ok(MqttClient {
            stream,
            client_id: self.client_id,
            keep_alive: self.keep_alive.unwrap_or(0),
            properties_builder: self.properties_builder,
            timeout: Duration::from_millis(100),
            packet_identifier: Default::default(),
        })
    }
}
