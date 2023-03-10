use std::fmt;
use std::net::SocketAddr;

use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::time::timeout;
use tokio::time::Duration;
use tokio_util::codec::Framed;
use tracing::trace;

use crate::identifier::PacketIdentifier;
use crate::v5::property::{PropertiesBuilder, SubscribeProperties};
use crate::v5::string::MqttString;
use crate::v5::types::{
    ConnAck, Connect, ControlPacket, Disconnect, MqttCodec, Publish, PublishResponse, QoS,
    ReasonCode, Retain, SubAck, Subscribe, SubscriptionOptions, Will,
};

pub struct MqttClient {
    stream: Framed<TcpStream, MqttCodec>,
    client_id: Option<MqttString>,
    keep_alive: u16,
    properties_builder: PropertiesBuilder,
    timeout: Duration,
    packet_identifier: PacketIdentifier,
    username: Option<MqttString>,
    password: Option<Bytes>,
    will: Option<Will>,
}

pub struct ClientBuilder<'a> {
    address: &'a str,
    client_id: Option<MqttString>,
    keep_alive: Option<u16>,
    timeout: Duration,
    properties_builder: PropertiesBuilder,
    username: Option<MqttString>,
    password: Option<Bytes>,
    will: Option<Will>,
}

impl<'a> MqttClient {
    pub fn builder(address: &'a str) -> ClientBuilder {
        ClientBuilder {
            address,
            client_id: None,
            keep_alive: None,
            timeout: Duration::from_millis(100),
            properties_builder: PropertiesBuilder::default(),
            username: None,
            password: None,
            will: None,
        }
    }

    pub async fn connect(&mut self, clean_start: bool) -> Result<ConnAck> {
        let connect = Connect {
            reserved: false,
            clean_start_flag: clean_start,
            keep_alive: self.keep_alive,
            properties: self.properties_builder.to_owned().connect(),
            client_identifier: self.client_id.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            will: self.will.to_owned(),
        };
        trace!("send {:?}", connect);
        self.stream.send(ControlPacket::Connect(connect)).await?;
        self.timeout_recv().await.and_then(|packet| match packet {
            ControlPacket::ConnAck(ack) => Ok(ack),
            _ => Err(anyhow!("unexpected: {}", packet)),
        })
    }

    pub async fn connect_reserved(&mut self, clean_start: bool) -> Result<ConnAck> {
        let connect = Connect {
            reserved: true,
            clean_start_flag: clean_start,
            keep_alive: self.keep_alive,
            properties: self.properties_builder.to_owned().connect(),
            client_identifier: self.client_id.clone(),
            username: None,
            password: None,
            will: self.will.to_owned(),
        };
        trace!("send {}", connect);
        self.stream.send(ControlPacket::Connect(connect)).await?;
        self.timeout_recv().await.and_then(|packet| match packet {
            ControlPacket::ConnAck(ack) => Ok(ack),
            _ => Err(anyhow!("unexpected: {}", packet)),
        })
    }

    pub async fn timeout_recv(&mut self) -> Result<ControlPacket, Error> {
        timeout(self.timeout, self.stream.next())
            .await
            .map_err(Error::msg)
            .and_then(|r| r.ok_or_else(|| anyhow!("disconnected")))
            .and_then(|r| r.map_err(Error::msg))
    }

    pub async fn recv(&mut self) -> Result<ControlPacket, Error> {
        self.stream
            .next()
            .await
            .transpose()
            .map_err(Error::msg)
            .and_then(|r| r.ok_or_else(|| anyhow!("none message")))
            .map_err(Error::msg)
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

        trace!("send {}", disconnect);
        self.stream
            .send(ControlPacket::Disconnect(disconnect))
            .await?;
        // expected None on socket close
        self.stream.next().await.transpose().map_err(Error::msg)
    }

    pub async fn publish_most_once(
        &mut self,
        topic_name: &str,
        payload: Vec<u8>,
        retain: bool,
    ) -> Result<()> {
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain,
            topic_name: MqttString::from(topic_name.to_owned()),
            packet_identifier: None,
            properties: Default::default(),
            payload: Bytes::from(payload),
        };
        trace!("send {}", publish);
        self.stream
            .send(ControlPacket::Publish(publish))
            .await
            .map_err(Error::msg)
    }

    pub async fn publish_least_once(
        &mut self,
        topic_name: &str,
        payload: Vec<u8>,
        retain: bool,
    ) -> Result<PublishResponse> {
        let packet_identifier = self.packet_identifier.next();
        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain,
            topic_name: MqttString::from(topic_name.to_owned()),
            packet_identifier,
            properties: Default::default(),
            payload: Bytes::from(payload),
        };
        trace!("send {}", publish);
        self.stream.send(ControlPacket::Publish(publish)).await?;
        self.timeout_recv().await.and_then(|packet| match packet {
            ControlPacket::PubAck(ack) => Ok(ack),
            unexpected => Err(anyhow!("unexpected: {}", unexpected)),
        })
    }

    pub async fn subscribe(&mut self, qos: QoS, topic_filter: &str) -> Result<SubAck> {
        let packet_identifier = self.packet_identifier.next();
        let subscribe = Subscribe {
            packet_identifier: packet_identifier.unwrap(),
            properties: SubscribeProperties::default(),
            topic_filters: vec![(
                MqttString::from(topic_filter.to_owned()),
                SubscriptionOptions {
                    qos,
                    nl: false,
                    rap: false,
                    retain: Retain::SendAtTime,
                },
            )],
        };
        trace!("send {}", subscribe);
        self.stream
            .send(ControlPacket::Subscribe(subscribe))
            .await?;
        self.timeout_recv().await.and_then(|packet| match packet {
            ControlPacket::SubAck(ack) => Ok(ack),
            _ => Err(anyhow!("unexpected: {}", packet)),
        })
    }

    pub async fn puback(&mut self, packet_identifier: u16) -> Result<()> {
        trace!("send PUBACK packet_identifier: {}", packet_identifier);
        self.stream
            .send(ControlPacket::PubAck(PublishResponse {
                packet_identifier,
                reason_code: ReasonCode::Success,
                properties: Default::default(),
            }))
            .await
            .map_err(Error::msg)
    }
}

impl fmt::Display for MqttClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.stream.get_ref().local_addr().unwrap())
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

    pub fn receive_maximum(mut self, value: u16) -> Self {
        self.properties_builder = self.properties_builder.receive_maximum(value).unwrap();
        self
    }

    pub fn with_will(mut self, will: Will) -> Self {
        self.will = Some(will);
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn username(mut self, value: &'static str) -> Self {
        self.username = Some(MqttString::from(value));
        self
    }

    pub fn password(mut self, value: &'static [u8]) -> Self {
        self.password = Some(Bytes::from_static(value));
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
        let stream = Framed::new(stream, MqttCodec::new(None));

        Ok(MqttClient {
            stream,
            client_id: self.client_id,
            keep_alive: self.keep_alive.unwrap_or(0),
            properties_builder: self.properties_builder,
            timeout: self.timeout,
            packet_identifier: Default::default(),
            username: self.username,
            password: self.password,
            will: self.will,
        })
    }
}
