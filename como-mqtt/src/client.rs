use std::net::SocketAddr;

use anyhow::{anyhow, Error, Result};
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use tokio::time::Duration;
use tokio_util::codec::Framed;

use crate::v5::property::PropertiesBuilder;
use crate::v5::types::{ConnAck, Connect, ControlPacket, MQTTCodec, MqttString};

pub struct Client {
    stream: Framed<TcpStream, MQTTCodec>,
    client_id: Option<MqttString>,
    keep_alive: u16,
    properties_builder: PropertiesBuilder,
}

pub struct ClientBuilder<'a> {
    address: &'a str,
    client_id: Option<MqttString>,
    keep_alive: Option<u16>,
    properties_builder: PropertiesBuilder,
}

impl Client {
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
        println!("send {:?}", connect);
        self.stream.send(ControlPacket::Connect(connect)).await;
        match timeout(Duration::from_millis(100), self.stream.next()).await {
            Ok(Some(Ok(result))) => match result {
                ControlPacket::ConnAck(ack) => Ok(ack),
                _ => Err(anyhow!("unexpected message")),
            },
            Ok(Some(Err(err))) => Err(anyhow!("error")),
            Ok(None) => Err(anyhow!("disconnected")),
            Err(elapsed) => Err(anyhow!("received response timeout")),
        }
    }

    pub async fn disconnect() -> Result<()> {
        Ok(())
    }

    pub async fn publish() -> Result<()> {
        Ok(())
    }

    pub async fn subscribe() -> Result<()> {
        Ok(())
    }
}

impl<'a> ClientBuilder<'a> {
    pub fn new(address: &'a str) -> Self {
        ClientBuilder {
            address,
            client_id: None,
            keep_alive: None,
            properties_builder: PropertiesBuilder::new(),
        }
    }

    pub fn client_id(mut self, value: String) -> Self {
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

    pub async fn client(self) -> Result<Client> {
        let peer: SocketAddr = self.address.parse()?;
        let socket = if peer.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };

        let stream = socket.connect(peer).await?;
        let stream = Framed::new(stream, MQTTCodec::new());

        Ok(Client {
            stream,
            client_id: self.client_id,
            keep_alive: self.keep_alive.unwrap_or(0),
            properties_builder: self.properties_builder,
        })
    }
}
