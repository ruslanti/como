use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tracing::{debug, instrument, warn};

use crate::mqtt::session::{ConnectionContextState, Session, SessionEvent};
use crate::mqtt::topic::Topics;
use crate::settings::Settings;

#[derive(Debug)]
pub(crate) struct AppContext {
    sessions: HashMap<String, (Sender<SessionEvent>, Sender<ConnectionContextState>)>,
    pub(crate) config: Arc<Settings>,
    topic_manager: Arc<Topics>,
    tx: mpsc::Sender<String>,
}

impl AppContext {
    pub fn new(config: Arc<Settings>, tx: mpsc::Sender<String>) -> Self {
        let path = config.topic.path.to_owned();
        Self {
            sessions: HashMap::new(),
            config,
            topic_manager: Arc::new(Topics::new(path)),
            tx,
        }
    }

    /*    pub async fn load(&mut self) -> Result<()> {
        self.topic_manager.load().await
    }*/

    #[instrument(skip(self))]
    pub fn clean(&mut self, identifier: String) {
        if self.sessions.remove(identifier.as_str()).is_some() {
            debug!("removed");
        }
    }

    //#[instrument(skip(self))]
    pub async fn connect(
        &mut self,
        identifier: &str,
    ) -> (Sender<SessionEvent>, Sender<ConnectionContextState>, bool) {
        if let Some((session_event_tx, connection_context_tx)) = self.sessions.get(identifier) {
            (
                session_event_tx.clone(),
                connection_context_tx.clone(),
                true,
            )
        } else {
            let (session_event_tx, session_event_rx) = mpsc::channel(32);
            let (connection_context_tx, connection_context_rx) = mpsc::channel(3);
            let mut session =
                Session::new(identifier, self.config.clone(), self.topic_manager.clone());
            let tx = self.tx.clone();
            let s = identifier.to_owned();
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
                identifier.to_string(),
                (session_event_tx.clone(), connection_context_tx.clone()),
            );
            (session_event_tx, connection_context_tx, false)
        }
    }
}
