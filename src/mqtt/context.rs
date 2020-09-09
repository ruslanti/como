use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::Result;
use futures::ready;
use tokio::stream::Stream;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::time::{delay_queue, DelayQueue, Error};
use tracing::{debug, error, instrument, trace, warn};
use uuid::Uuid;

use crate::mqtt::proto::types::{ControlPacket, MqttString};
use crate::mqtt::session::SessionEvent::Disconnect;
use crate::mqtt::session::{Session, SessionEvent};
use crate::mqtt::topic::Topic;
use crate::settings::{ConnectionSettings, Settings};

type SessionSender = Sender<SessionEvent>;
pub(crate) type SessionContext = Option<(String, SessionSender, Option<u32>)>;

#[derive(Debug)]
pub(crate) struct AppContext {
    sessions: HashMap<String, SessionSender>,
    sessions_expire: DelayQueue<String>,
    pub(crate) config: Arc<Settings>,
    topic_manager: Arc<RwLock<Topic>>,
}

impl AppContext {
    pub fn new(config: Arc<Settings>) -> Self {
        Self {
            sessions: HashMap::new(),
            sessions_expire: DelayQueue::new(),
            config,
            topic_manager: Arc::new(RwLock::new(Topic::new("".to_string()))),
        }
    }

    pub async fn connect_session(
        &mut self,
        key: &str,
        conn_tx: Sender<ControlPacket>,
    ) -> SessionSender {
        if let Some(session_tx) = self.sessions.get(key) {
            //TODO close existing connection
            session_tx.clone()
        } else {
            let (tx, rx) = mpsc::channel(32);
            let mut session = Session::new(
                key,
                rx,
                conn_tx,
                self.config.clone(),
                self.topic_manager.clone(),
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

    pub async fn disconnect_session(&mut self, key: &str, expire: Option<u32>) {
        /*        if let Some(expire) = expire {
            self.sessions_expire.insert(key.to_string(), Duration::from_secs(expire as u64));
        } else {*/
        self.sessions.remove(key);
        /*        }*/
    }

    /*    fn poll_purge(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
            while let Some(res) = ready!(self.sessions_expire.poll_expired(cx)) {
                let entry = res?;
                self.sessions.remove(entry.get_ref());
            }
            Poll::Ready(Ok(()))
        }
    */
}
