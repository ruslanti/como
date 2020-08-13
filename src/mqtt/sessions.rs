use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::stream::StreamExt;
use tracing::{trace, debug, error, instrument};
use anyhow::{anyhow, Result, Context};
use crate::mqtt::shutdown::Shutdown;
use crate::mqtt::proto::types::{ControlPacket, Connect, ConnAck, ReasonCode};
use uuid::Uuid;
use bytes::Bytes;
use crate::mqtt::proto::property::ConnAckProperties;
use crate::mqtt::connection::State;
use crate::settings::ConnectionSettings;

pub struct SessionManager {
    sessions_states_rx: mpsc::Receiver<(Command)>,
    sessions: HashMap<String, Session>,
    config: ConnectionSettings
}

struct Session {
}

#[derive(Debug)]
pub enum Command {
    Connect(String),
    Subscribe,
    Unsubscribe,
    Disconnect
}


impl SessionManager {

    pub fn new(config: ConnectionSettings, sessions_states_rx: mpsc::Receiver<(Command)>) -> Self {
        SessionManager {
            sessions_states_rx,
            sessions: HashMap::new(),
            config
        }
    }

    async fn connect(&mut self, identifier: String) -> Result<()> {
        let session = Session {};
        let old = self.sessions.insert(identifier.clone(), session);
        if let Some(s) = old {
            //todo disconnect old session
        }



/*        let ack = ControlPacket::ConnAck(ConnAck{
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

        self.state = State::Established {
            client_identifier: identifier,
            keep_alive: msg.keep_alive,
            properties: msg.properties,
            will: msg.will
        };*/

        Ok(())
    }


    async fn process(&mut self) -> Result<()> {
        while let Some(msg) =  self.sessions_states_rx.next().await {
            trace!("got = {:?}", msg);

/*            match msg {
                Command::Connect(client_identifier) => self.connect(client_identifier),
                //ControlPacket::Connect(connect) => { self.connect(connect)},
                _ => unreachable!()
            }*/
        }

        Ok(())
    }

    pub(crate) async fn run(&mut self, mut shutdown: Shutdown) -> Result<()> {
        trace!("run start");
        while !shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown signal.
            tokio::select! {
                res = self.process() => {
                    res?; // handle error
                },
                _ = shutdown.recv() => {
                    trace!("shutdown received");

                    return Ok(());
                }
            }
            ;
        }
        trace!("run end");
        Ok(())
    }
}