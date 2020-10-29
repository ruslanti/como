use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Error;
use futures::TryFutureExt;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::timeout;
use tokio::time::Duration;
use tracing::{trace, warn};

use crate::mqtt::proto::types::{ControlPacket, Disconnect, ReasonCode, Will};
use crate::mqtt::session::{Session, SessionEvent};
use crate::mqtt::topic::TopicManager;
use crate::settings::Settings;

type SessionSender = Sender<SessionEvent>;
pub(crate) type SessionContext = Option<(String, SessionSender, Option<u32>)>;

#[derive(Debug)]
pub(crate) struct AppContext {
    sessions: HashMap<String, SessionSender>,
    expires: HashMap<String, oneshot::Sender<()>>,
    pub(crate) config: Arc<Settings>,
    topic_manager: Arc<RwLock<TopicManager>>,
}

impl AppContext {
    pub fn new(config: Arc<Settings>) -> Self {
        Self {
            sessions: HashMap::new(),
            // sessions_expire: DelayQueue::new(),
            expires: HashMap::new(),
            config,
            topic_manager: Arc::new(RwLock::new(TopicManager::new())),
        }
    }

    pub async fn connect(
        &mut self,
        key: &str,
        clean_start: bool,
        conn_tx: Sender<ControlPacket>,
        will: Option<Will>,
    ) -> SessionSender {
        self.expires.remove(key);
        //TODO close existing connection
        if clean_start {
            if let Some(tx) = self.sessions.remove(key) {
                let event = SessionEvent::Disconnect(Disconnect {
                    reason_code: ReasonCode::SessionTakenOver,
                    properties: Default::default(),
                });
                if let Err(err) = tx.clone().send(event).map_err(Error::msg).await {
                    warn!(cause = ?err, "session error");
                }
            }
        }

        if let Some(tx) = self.sessions.get(key) {
            tx.clone()
        } else {
            let (tx, rx) = mpsc::channel(32);
            let mut session = Session::new(
                key,
                rx,
                conn_tx,
                self.config.clone(),
                self.topic_manager.clone(),
                will,
            );
            tokio::spawn(async move {
                if let Err(err) = session.session().await {
                    warn!(cause = ?err, "session error");
                }
            });
            self.sessions.insert(key.to_string(), tx.clone());
            tx
        }
    }

    pub async fn disconnect(&mut self, key: &str, expire: Option<u32>) {
        if let Some(expire) = expire {
            let (tx, rx) = oneshot::channel();
            self.expires.insert(key.to_owned(), tx);
            if let Err(_) = timeout(Duration::from_secs(expire as u64), rx).await {
                self.expires.remove(key);
                self.sessions.remove(key);
            }
        } else {
            self.sessions.remove(key);
        }
    }
}
