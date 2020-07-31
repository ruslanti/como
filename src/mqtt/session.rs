use anyhow::{anyhow, Result, Context};
use tracing::{trace, debug, error, instrument};
use crate::mqtt::proto::types::{ControlPacket, ReasonCode, MQTTCodec, Will, Connect, Publish, ConnAck, QoS, PubAck};
use crate::mqtt::proto::property::{ConnAckProperties, DisconnectProperties, ConnectProperties, PubResProperties};
use crate::settings::ConnectionSettings;
use bytes::Bytes;
use uuid::Uuid;
use tokio_util::codec::Framed;
//use tokio::stream::StreamExt;
use futures::{SinkExt, StreamExt};
use crate::mqtt::shutdown::Shutdown;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, Semaphore};
use std::sync::Arc;


enum ExactlyOnceState {
    Received,
    Released,
}

#[derive(Debug)]
enum State {
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
pub struct Session {
    stream: Framed<TcpStream, MQTTCodec>,
    limit_connections: Arc<Semaphore>,
    shutdown_complete: mpsc::Sender<()>,
    config: ConnectionSettings,
    //fsm: SessionFsm
    /*    sessions_notify_tx: broadcast::Sender<Bytes>,
        sessions_notify_rx: broadcast::Receiver<Bytes>,*/
    state: State,
}

impl Session {
    pub fn new(stream: Framed<TcpStream, MQTTCodec>, limit_connections: Arc<Semaphore>,
               shutdown_complete: mpsc::Sender<()>, config: ConnectionSettings) -> Session {
        /*        tokio::spawn(async move {
                    let r = sessions_notify_rx.recv().await;
                    debug!("notify receive: {:?}", r)
                });*/
        Session {
            stream,
            limit_connections,
            shutdown_complete,
            config,
            state: State::Idle,
            /*            sessions_notify_tx,
                        sessions_notify_rx,*/
        }
    }

    async fn process_connect(&mut self, msg: Connect) -> Result<()> {
        let (assigned_client_identifier, identifier) = if let Some(id) = msg.client_identifier {
            /*                    if let Ok(sent) = self.sessions_notify_tx.send(id.clone()) {
                                    debug!("send session notify to: {} receivers", sent);
                                };*/
            (None, id)
        } else {
            let id: Bytes = Uuid::new_v4().to_hyphenated().to_string().into();
            (Some(id.clone()), id)
        };

        let ack = ControlPacket::ConnAck(ConnAck{
            session_present: false,
            reason_code: ReasonCode::Success,
            properties: ConnAckProperties {
                session_expire_interval: self.config.session_expire_interval,
                receive_maximum: self.config.receive_maximum,
                maximum_qos: self.config.maximum_qos,
                retain_available: self.config.retain_available,
                maximum_packet_size: self.config.maximum_packet_size,
                assigned_client_identifier,
                topic_alias_maximum: self.config.topic_alias_maximum,
                reason_string: None,
                user_properties: vec![],
            }
        });

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
                    let ack = ControlPacket::PubAck(PubAck {
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
                todo!("exactly once")
            },
        }
        Ok(())
    }

    async fn process_packet(&mut self, packet: ControlPacket) -> Result<()> {
        trace!("recv {:?}", packet);
        match (&self.state, packet) {
            (State::Idle, ControlPacket::Connect(connect)) => self.process_connect(connect).await,
            (State::Idle, packet) => {
                error!("unacceptable packet {:?} in idle state", packet);
                Err(anyhow!("unacceptable event").context(""))
            },
            (State::Established{..}, ControlPacket::Publish(publish)) => self.process_publish(publish).await,
            (State::Established{..}, ControlPacket::Disconnect(disconnect)) => {
                debug!("disconnect reason code: {:?}, reason string:{:?}", disconnect.reason_code, disconnect.properties.reason_string);
                self.state = State::Disconnected;
                self.stream.close().await?;
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
        while let Some(packet) = self.stream.next().await {
            match packet {
                Ok(packet) => {
                    self.process_packet(packet).await?;
                }
                Err(e) => {
                    error!("error on process connection: {}", e);
                    return Err(e.into());
                }
            }
        };
        trace!("process end");
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn disconnect(&mut self) {
        debug!("send disconnect");
       // self.stream.send()
        // todo send disconnect to session
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
                    self.disconnect().await;
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

impl Drop for Session {
    fn drop(&mut self) {
        self.limit_connections.add_permits(1);
    }
}
