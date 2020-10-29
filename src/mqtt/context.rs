use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Error;
use futures::TryFutureExt;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, instrument, warn};

use crate::mqtt::proto::types::{Disconnect, ReasonCode};
use crate::mqtt::session::{ConnectionContextState, Session, SessionEvent};
use crate::mqtt::topic::TopicManager;
use crate::settings::Settings;

#[derive(Debug)]
pub(crate) struct AppContext {
    sessions: HashMap<String, (Sender<SessionEvent>, Sender<ConnectionContextState>)>,
    pub(crate) config: Arc<Settings>,
    topic_manager: Arc<RwLock<TopicManager>>,
    tx: mpsc::Sender<String>,
}

impl AppContext {
    pub fn new(config: Arc<Settings>, tx: mpsc::Sender<String>) -> Self {
        Self {
            sessions: HashMap::new(),
            config,
            topic_manager: Arc::new(RwLock::new(TopicManager::new())),
            tx,
        }
    }

    #[instrument(skip(self))]
    pub fn clean(&mut self, identifier: String) {
        if let Some(_) = self.sessions.remove(identifier.as_str()) {
            debug!("removed");
        }
    }

    pub async fn connect(
        &mut self,
        key: &str,
        clean_start: bool,
    ) -> (Sender<SessionEvent>, Sender<ConnectionContextState>) {
        //TODO close existing connection
        if clean_start {
            if let Some((session_event_tx, _)) = self.sessions.remove(key) {
                let event = SessionEvent::Disconnect(Disconnect {
                    reason_code: ReasonCode::SessionTakenOver,
                    properties: Default::default(),
                });
                if let Err(err) = session_event_tx.send(event).map_err(Error::msg).await {
                    warn!(cause = ?err, "session error");
                }
            }
        }

        if let Some((session_event_tx, connection_context_tx)) = self.sessions.get(key) {
            (session_event_tx.clone(), connection_context_tx.clone())
        } else {
            let (session_event_tx, session_event_rx) = mpsc::channel(32);
            let (connection_context_tx, connection_context_rx) = mpsc::channel(3);
            let mut session = Session::new(key, self.config.clone(), self.topic_manager.clone());
            let tx = self.tx.clone();
            let s = key.to_owned();
            tokio::spawn(async move {
                if let Err(err) = session
                    .session(session_event_rx, connection_context_rx)
                    .await
                {
                    //FIXME handle session end and disconnect connection
                    warn!(cause = ?err, "session error");
                };
                if let Err(err) = tx.send(s).await {
                    debug!(cause = ?err, "session clear error");
                }
            });
            self.sessions.insert(
                key.to_string(),
                (session_event_tx.clone(), connection_context_tx.clone()),
            );
            (session_event_tx, connection_context_tx)
        }
    }
}
