use anyhow::Result;
use tracing::{trace, debug, error, instrument};
use crate::mqtt::proto::types::{ControlPacket, ReasonCode, MQTTCodec};
use crate::mqtt::proto::property::{ConnAckProperties, DisconnectProperties};
use crate::settings::Connection;
use bytes::Bytes;
use uuid::Uuid;
use tokio_util::codec::Framed;
use tokio::stream::StreamExt;
use futures::SinkExt;
use crate::mqtt::shutdown::Shutdown;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, Semaphore};
use std::sync::Arc;

/*#[derive(Debug)]
enum State {
    Idle,
    Connected {
        client_identifier: Bytes,
    }
}*/

struct Idle {
}

struct Connected {
    client_identifier: Bytes,
}

#[derive(Debug)]
pub struct Session {
    config: Connection,
/*    sessions_notify_tx: broadcast::Sender<Bytes>,
    sessions_notify_rx: broadcast::Receiver<Bytes>,*/
    //state: S
}

impl Session {
    pub fn new(config: Connection/*, sessions_notify_tx: broadcast::Sender<Bytes>, mut sessions_notify_rx: broadcast::Receiver<Bytes>*/) -> Session {
/*        tokio::spawn(async move {
            let r = sessions_notify_rx.recv().await;
            debug!("notify receive: {:?}", r)
        });*/
        Session {
            config,
/*            sessions_notify_tx,
            sessions_notify_rx,*/
        }
    }

    pub fn handle_event(&self, packet: ControlPacket) -> Result<Option<ControlPacket>> {
        match packet {
            ControlPacket::Connect {
                clean_start_flag,
                keep_alive,
                properties,
                client_identifier,
                username,
                password,
                will
            } => {
                let (assigned_client_identifier, identifier) = if let Some(id) = client_identifier {
/*                    if let Ok(sent) = self.sessions_notify_tx.send(id.clone()) {
                        debug!("send session notify to: {} receivers", sent);
                    };*/
                    (None, id)
                } else {
                    let id: Bytes = Uuid::new_v4().to_hyphenated().to_string().into();
                    (Some(id.clone()), id)
                };

                let ack = ControlPacket::ConnAck {
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
                        user_properties: vec![]
                    }
                };
                /*                self.state = State::Connected {
                    client_identifier: identifier
                };*/
                // self.state = Session::<Connected>::from(self);

                Ok(Some(ack))
            },
            /*            ControlPacket::Publish {
                dup, qos, retain,
                topic_name,
                packet_identifier,
                properties,
                payload } => {
                Ok(None)
            },
            ControlPacket::Disconnect {..} => {
                Ok(None)
            },*/
            _ => unreachable!()
        }
    }
}


/*
impl From<Session<Idle>> for Session<Connected> {
    fn from(value: Session<Idle>) -> Self {
        Session {
            sessions_notify_tx: value.sessions_notify_tx,
            config: value.config,
            state: Connected{ client_identifier: Default::default() }
        }
    }
}*/
