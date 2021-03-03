use std::net::SocketAddr;

use anyhow::Result;
use futures::SinkExt;
use tokio::net::{TcpSocket, TcpStream};
use tokio_util::codec::Framed;

use crate::v5::property::PropertiesBuilder;
use crate::v5::types::{Connect, ControlPacket, MQTTCodec, MqttString};

struct Client {
    stream: Framed<TcpStream, MQTTCodec>,
    client_id: Option<MqttString>,
    keep_alive: u16,
    properties_builder: PropertiesBuilder,
}

struct ClientBuilder<'a> {
    address: &'a str,
    client_id: Option<MqttString>,
    keep_alive: Option<u16>,
    properties_builder: PropertiesBuilder,
}

impl Client {
    async fn connect(&mut self, clean_start: bool) -> Result<()> {
        let connect = Connect {
            clean_start_flag: clean_start,
            keep_alive: self.keep_alive,
            properties: self.properties_builder.to_owned().connect(),
            client_identifier: self.client_id.to_owned(),
            username: None,
            password: None,
            will: None,
        };
        self.stream.send(ControlPacket::Connect(connect)).await
        //let res = self.stream.next().await
    }

    async fn disconnect() -> Result<()> {
        Ok(())
    }

    async fn publish() -> Result<()> {
        Ok(())
    }

    async fn subscribe() -> Result<()> {
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
