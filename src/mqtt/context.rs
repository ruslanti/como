use anyhow::{bail, Error, Result};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::{debug, instrument, warn};

use crate::mqtt::proto::types::{Connect, ControlPacket};
use crate::mqtt::session::{Session, SessionState};
use crate::mqtt::topic::Topics;
use crate::settings::Settings;
use sled::{Db, Tree};
use std::net::SocketAddr;

#[derive(Debug)]
pub(crate) struct AppContext {
    db: Db,
    sessions: Tree,
    subscriptions: Tree,
    pub(crate) config: Arc<Settings>,
    topic_manager: Arc<Topics>,
}

impl AppContext {
    pub fn new(config: Arc<Settings>) -> Result<Self> {
        let sessions_db_path = config.connection.db_path.to_owned();
        let topics_db_path = config.topic.db_path.to_owned();
        let db = sled::open(sessions_db_path)?;
        let sessions = db.open_tree("sessions")?;
        let subscriptions = db.open_tree("subscriptions")?;
        Ok(Self {
            db,
            sessions,
            subscriptions,
            config,
            topic_manager: Arc::new(Topics::new(topics_db_path)?),
        })
    }

    /*    pub async fn load(&mut self) -> Result<()> {
        self.topic_manager.load().await
    }*/

    #[instrument(skip(self))]
    pub fn clean(&mut self, identifier: String) {
        /* if self.sessions.remove(identifier.as_str()).is_some() {
            debug!("removed");
        }*/
    }

    pub fn make_session(
        &self,
        session: &str,
        connection_reply_tx: Sender<ControlPacket>,
        msg: Connect,
    ) -> Session {
        Session::new(
            session,
            connection_reply_tx,
            msg.properties,
            msg.will,
            self.config.clone(),
            self.topic_manager.clone(),
            self.subscriptions,
        )
    }

    //#[instrument(skip(self))]
    pub fn accuire_session(&mut self, session: &str, peer: SocketAddr) -> Result<SessionState> {
        let mut session_present = false;

        let update_fn = |old: Option<&[u8]>| -> Option<Vec<u8>> {
            let session = match old {
                Some(encoded) => {
                    if let Ok(session) = bincode::deserialize(encoded) {
                        session_present = true;
                        //session.peer = peer;
                        Some(session)
                    } else {
                        Some(SessionState::new(peer))
                    }
                }
                None => Some(SessionState::new(peer)),
            };
            session.and_then(|s: SessionState| bincode::serialize(&s).ok())
        };

        if let Some(encoded) = self
            .sessions
            .fetch_and_update(session, update_fn)
            .map_err(Error::msg)?
        {
            bincode::deserialize(encoded.as_ref()).map_err(Error::msg)
        } else {
            bail!("could not store session");
        }
    }
}
