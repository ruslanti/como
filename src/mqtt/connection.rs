use anyhow::{anyhow, Result, Context};
use tracing::{trace, debug, error, instrument};
use crate::mqtt::proto::types::{ControlPacket, ReasonCode, MQTTCodec, Will, Connect, Publish, ConnAck, QoS, PubRes, Disconnect};
use crate::mqtt::proto::property::{ConnAckProperties, DisconnectProperties, ConnectProperties, PubResProperties};
use crate::settings::ConnectionSettings;
use bytes::Bytes;
use uuid::Uuid;
use tokio_util::codec::Framed;
use futures::{SinkExt, StreamExt};
use crate::mqtt::shutdown::Shutdown;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, Semaphore};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use std::ops::Add;
use crate::mqtt::proto::types::ControlPacket::PubComp;


enum ExactlyOnceState {
    Received,
    Released,
}

#[derive(Debug)]
pub(crate) enum State {
    Idle,
    Established{
        client_identifier: Bytes,
        keep_alive: u16,
        properties: ConnectProperties,
        will: Option<Will>
    },
    Disconnected
}


#[derive(Debug)]
pub struct ConnectionHandler {
    stream: Framed<TcpStream, MQTTCodec>,
    limit_connections: Arc<Semaphore>,
    sessions_states_tx: mpsc::Sender<(ControlPacket)>,
    shutdown_complete: mpsc::Sender<()>,
    config: ConnectionSettings,
    state: State,
    connection_rx: mpsc::Receiver<()>,
    stream_timeout: Option<Duration>
}

impl ConnectionHandler {
    pub fn new(stream: Framed<TcpStream, MQTTCodec>, limit_connections: Arc<Semaphore>,
               sessions_states_tx: mpsc::Sender<(ControlPacket)>,
               shutdown_complete: mpsc::Sender<()>,
               config: ConnectionSettings) -> ConnectionHandler {
        let (connection_tx, connection_rx) = mpsc::channel(32);
        ConnectionHandler {
            stream,
            limit_connections,
            sessions_states_tx,
            shutdown_complete,
            config,
            state: State::Idle,
            connection_rx,
            stream_timeout: Some(Duration::from_millis(config.idle_keep_alive as u64))
        }
    }

    async fn process_connect(&mut self, msg: Connect) -> Result<()> {
        let (assigned_client_identifier, identifier) = if let Some(id) = msg.client_identifier {
            (None, id)
        } else {
            let id: Bytes = Uuid::new_v4().to_hyphenated().to_string().into();
            (Some(id.clone()), id)
        };

        let maximum_qos = if let Some(QoS::ExactlyOnce) = self.config.maximum_qos { None } else { self.config.maximum_qos };
        let ack = ControlPacket::ConnAck(ConnAck{
            session_present: false,
            reason_code: ReasonCode::Success,
            properties: ConnAckProperties {
                session_expire_interval: self.config.session_expire_interval,
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
                authentication_data: None
            }
        });

        let client_keep_alive = if msg.keep_alive != 0 { Some(msg.keep_alive) } else { None };
        self.stream_timeout = self.config.server_keep_alive.or(client_keep_alive).map(|d| Duration::from_secs(d as u64));

        trace!("send {:?}", ack);
        self.stream.send(ack).await?;

        self.state = State::Established {
            client_identifier: identifier,
            keep_alive: msg.keep_alive,
            properties: msg.properties,
            will: msg.will
        };

        Ok(())
    }

    async fn process_publish(&mut self, msg: Publish) -> Result<()> {
        match msg.qos {
            QoS::AtMostOnce => {
                //todo store topic
            },
            QoS::AtLeastOnce => {
                if let Some(packet_identifier) = msg.packet_identifier
                {
                    //todo store topic
                    let ack = ControlPacket::PubAck(PubRes {
                        packet_identifier,
                        reason_code: ReasonCode::Success,
                        properties: PubResProperties { reason_string: None, user_properties: vec![] }
                    });
                    trace!("send {:?}", ack);
                    self.stream.send(ack).await?;
                } else {
                    return Err(anyhow!("undefined packet_identifier"))
                }
            }
            QoS::ExactlyOnce => {
                if let Some(packet_identifier) = msg.packet_identifier
                {
                    //todo store topic
                    let ack = ControlPacket::PubRec(PubRes {
                        packet_identifier,
                        reason_code: ReasonCode::Success,
                        properties: PubResProperties { reason_string: None, user_properties: vec![] }
                    });
                    trace!("send {:?}", ack);
                    self.stream.send(ack).await?;
                } else {
                    return Err(anyhow!("undefined packet_identifier"))
                }
            },
        }
        Ok(())
    }

    async fn process_pubrel(&mut self, msg: PubRes) -> Result<()> {
        let comp = ControlPacket::PubComp(PubRes{
            packet_identifier: msg.packet_identifier,
            reason_code: ReasonCode::Success,
            properties: PubResProperties { reason_string: None, user_properties: vec![] }
        });
        trace!("send {:?}", comp);
        self.stream.send(comp).await?;
        Ok(())
    }

    async fn process_packet(&mut self, packet: ControlPacket) -> Result<()> {
        trace!("recv {:?}", packet);
        match (&self.state, packet) {
            (State::Idle{..}, ControlPacket::Connect(connect)) => self.process_connect(connect).await,
            (State::Idle{..}, packet) => {
                error!("unacceptable packet {:?} in idle state", packet);
                Err(anyhow!("unacceptable event").context(""))
            },
            (State::Established{..}, ControlPacket::Publish(publish)) => self.process_publish(publish).await,
            (State::Established{..}, ControlPacket::PubRel(pubrel)) => self.process_pubrel(pubrel).await,
            (State::Established{..}, ControlPacket::Disconnect(disconnect)) => {
                debug!("disconnect reason code: {:?}, reason string:{:?}", disconnect.reason_code, disconnect.properties.reason_string);
                self.state = State::Disconnected;
                self.stream.close().await?;
                Ok(())
            },
            (State::Established{..}, ControlPacket::PingReq) => {
                debug!("ping req");
                self.stream.send(ControlPacket::PingResp).await?;
                Ok(())
            },
            (State::Established{..}, packet) => {
                error!("unacceptable packet {:?} in established state", packet);
                Err(anyhow!("unacceptable event").context(""))
            },
            _ => unreachable!()
        }
    }

    #[instrument(skip(self))]
    pub async fn process_stream(&mut self) -> Result<()> {
        trace!("process start");
        loop {
            trace!("stream timeout  {:?}", self.stream_timeout);
            match self.stream_timeout {
                Some(duration) => {
                    match timeout(duration.add(Duration::from_millis(100)), self.process_next()).await {
                        Ok(res) => {
                            if !res? {
                                break
                            }
                        },
                        Err(e) =>  {
                            error!("timeout on process connection: {}", e);
                            // handle timeout
                            match self.state {
                                State::Idle => return Err(e.into()),
                                State::Established{..} => {
                                    self.disconnect(ReasonCode::KeepAliveTimeout).await?;
                                    break;
                                },
                                _ => unreachable!()
                            }

                        }
                    }
                },
                None => {
                    if !self.process_next().await? {
                        break
                    }
                }
            }
        }
        trace!("process end");
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn process_next(&mut self) -> Result<bool> {
        trace!("process next start");
        if let Some(packet) = self.stream.next().await {
            match packet {
                Ok(packet) => {
                    self.process_packet(packet).await?;
                }
                Err(e) => {
                    error!("error on process connection: {}", e);
                    return Err(e.into());
                }
            }
            trace!("process next end");
            Ok(true)
        } else {
            trace!("process next disconnected");
            Ok(false)
        }
    }

    #[instrument(skip(self))]
    pub async fn disconnect(&mut self, reason_code: ReasonCode) -> Result<()> {
        debug!("send disconnect");

        let disc = ControlPacket::Disconnect(Disconnect{
            reason_code,
            properties: DisconnectProperties {
                session_expire_interval: None,
                reason_string: None,
                user_properties: vec![],
                server_reference: None
            }
        });
        trace!("send {:?}", disc);
        self.stream.send(disc).await?;
        Ok(())
    }

    //#[instrument(skip(self))]
    pub(crate) async fn run(&mut self, mut shutdown: Shutdown) -> Result<()> {
        trace!("run start");
        while !shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown signal.
            tokio::select! {
                res = self.process_stream() => {
                    res?; // handle error
                    break; // disconnect
                },
                _ = shutdown.recv() => {
                    trace!("shutdown received");
                    self.disconnect(ReasonCode::ServerShuttingDown).await?;
                    trace!("disconnect sent");
                    return Ok(());
                }
            }
            ;
        }
        trace!("run end");
        Ok(())
    }
}

impl Drop for ConnectionHandler {
    fn drop(&mut self) {
        self.limit_connections.add_permits(1);
    }
}
