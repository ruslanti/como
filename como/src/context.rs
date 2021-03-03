use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use sled::{Db, Tree};
use tokio::sync::mpsc::Sender;

use como_mqtt::v5::property::ConnectProperties;
use como_mqtt::v5::types::{ControlPacket, MqttString, Will};

use crate::session::Session;
use crate::settings::Settings;
use crate::topic::Topics;

#[derive(Debug)]
pub struct AppContext {
    db: Db,
    sessions_db: Tree,
    subscriptions_db: Tree,
    pub config: Arc<Settings>,
    topic_manager: Arc<Topics>,
}

impl AppContext {
    pub fn new(config: Arc<Settings>) -> Result<Self> {
        let sessions_db_path = config.connection.db_path.as_str();
        let topics_db_path = config.topics.db_path.as_str();
        let db = sled::open(sessions_db_path)?;
        let sessions = db.open_tree("sessions")?;
        let subscriptions = db.open_tree("subscriptions")?;
        Ok(Self {
            db,
            sessions_db: sessions,
            subscriptions_db: subscriptions,
            config: config.clone(),
            topic_manager: Arc::new(Topics::new(topics_db_path)?),
        })
    }

    pub fn make_session(
        &self,
        session: MqttString,
        response_tx: Sender<ControlPacket>,
        peer: SocketAddr,
        properties: ConnectProperties,
        will: Option<Will>,
    ) -> Session {
        Session::new(
            session,
            response_tx,
            peer,
            properties,
            will,
            self.config.connection.to_owned(),
            self.topic_manager.clone(),
            self.sessions_db.clone(),
            self.subscriptions_db.clone(),
        )
    }
}
