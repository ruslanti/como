use std::borrow::Borrow;
use std::convert::{TryFrom, TryInto};
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use anyhow::{Context, Error};
use serde::{Deserialize, Serialize};
use sled::{Batch, Event, IVec, Tree};
use tokio::sync::mpsc::Sender;
use tracing::{debug, info, trace, warn};

use como_mqtt::v5::types::SubscriptionOptions;

use crate::session::SessionEvent;
use crate::settings::Settings;
use crate::topic::Topics;

pub(crate) trait SessionStore {
    fn acquire(
        &self,
        client_id: &str,
        peer: SocketAddr,
        clean_start: bool,
    ) -> Result<Option<SessionState>>;
    fn update(&self, client_id: &str, session_state: SessionState) -> Result<()>;
    fn remove(&self, client_id: &str) -> Result<()>;
    fn start_monitor(
        &self,
        client_id: &str,
        peer: SocketAddr,
        session_event_tx: Sender<SessionEvent>,
    );
}

pub(crate) trait SubscriptionsStore {
    fn get_subscription(
        &self,
        client_id: &str,
        topic_filter: &[u8],
    ) -> Result<Option<SubscriptionOptions>>;
    fn store_subscription(
        &self,
        client_id: &str,
        topic_filter: &[u8],
        option: &SubscriptionOptions,
    ) -> Result<()>;
    fn remove_subscription(&self, client_id: &str, topic_filter: &[u8]) -> Result<bool>;
    fn list_subscriptions(&self, client_id: &str) -> Result<Vec<(IVec, SubscriptionOptions)>>;
    fn clear_subscriptions(&self, client_id: &str) -> Result<usize>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionState {
    pub peer: SocketAddr,
    pub expire: Option<i64>,
    pub last_topic_id: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct SessionContext {
    pub settings: Arc<Settings>,
    pub topic_manager: Arc<Topics>,
    sessions: Tree,
    subscriptions: Tree,
}

impl SessionState {
    pub fn new(peer: SocketAddr) -> Self {
        SessionState {
            peer,
            expire: None,
            last_topic_id: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SubscriptionKey<'a> {
    session: &'a [u8],
    topic_filter: &'a [u8],
}

impl TryFrom<IVec> for SessionState {
    type Error = Error;

    fn try_from(encoded: IVec) -> anyhow::Result<Self> {
        bincode::deserialize(encoded.as_ref()).map_err(Error::msg)
    }
}

impl TryFrom<&[u8]> for SessionState {
    type Error = Error;

    fn try_from(encoded: &[u8]) -> anyhow::Result<Self> {
        bincode::deserialize(encoded).map_err(Error::msg)
    }
}

impl TryInto<Vec<u8>> for SessionState {
    type Error = Error;

    fn try_into(self) -> anyhow::Result<Vec<u8>> {
        bincode::serialize(&self).map_err(Error::msg)
    }
}

impl SessionContext {
    pub fn new(settings: Arc<Settings>) -> Result<Self> {
        let db = sled::Config::new().temporary(true).create_new(true);
        let db = if let Some(path) = settings.connection.db_path.borrow() {
            db.path(path).open()?
        } else {
            db.open()?
        };
        let sessions = db.open_tree("sessions")?;
        let subscriptions = db.open_tree("subscriptions")?;

        let topic_manager = Arc::new(Topics::new(settings.topics.clone())?);

        Ok(SessionContext {
            settings,
            topic_manager,
            sessions,
            subscriptions,
        })
    }
}

impl SessionStore for SessionContext {
    fn acquire(
        &self,
        client_id: &str,
        peer: SocketAddr,
        clean_start: bool,
    ) -> Result<Option<SessionState>> {
        let update_fn = |old: Option<&[u8]>| -> Option<Vec<u8>> {
            let session_state = match old {
                Some(encoded) => {
                    if let Ok(mut session_state) = SessionState::try_from(encoded) {
                        session_state.peer = peer;
                        session_state.expire = None;
                        if clean_start {
                            session_state.last_topic_id = None;
                        }
                        session_state
                    } else {
                        SessionState::new(peer)
                    }
                }
                None => SessionState::new(peer),
            };
            session_state.try_into().ok()
        };

        if let Some(encoded) = self
            .sessions
            .fetch_and_update(client_id, update_fn)
            .map_err(Error::msg)?
        {
            Ok(Some(SessionState::try_from(encoded)?))
        } else {
            Ok(None)
        }
    }

    fn update(&self, client_id: &str, state: SessionState) -> Result<()> {
        let update_fn = |old: Option<&[u8]>| -> Option<Vec<u8>> {
            let session_state = match old {
                Some(encoded) => {
                    if let Ok(mut session_state) = SessionState::try_from(encoded) {
                        if state.peer == session_state.peer {
                            session_state.expire = state.expire;
                            session_state.last_topic_id = state.last_topic_id;
                            Some(session_state)
                        } else {
                            debug!("session {:?} acquired by {}", client_id, session_state.peer);
                            None
                        }
                    } else {
                        None
                    }
                }
                None => None,
            };
            session_state.and_then(|s| s.try_into().ok())
        };

        self.sessions
            .update_and_fetch(client_id, update_fn)
            .map(|_| ())
            .map_err(Error::msg)
    }

    fn remove(&self, client_id: &str) -> Result<()> {
        self.sessions
            .remove(client_id)
            .map(|_| ())
            .map_err(Error::msg)
    }

    fn start_monitor(
        &self,
        client_id: &str,
        peer: SocketAddr,
        session_event_tx: Sender<SessionEvent>,
    ) {
        let mut subscriber = self.sessions.watch_prefix(client_id);
        let client_id = client_id.to_owned();
        tokio::spawn(async move {
            while let Some(event) = (&mut subscriber).await {
                match event {
                    Event::Insert { key, value } => {
                        if key == client_id.as_bytes() {
                            if let Ok(state) = SessionState::try_from(value) {
                                // if peer address is different then session is acquired by other
                                // connection, else it is closed by the same connection and break
                                // the watcher
                                if peer != state.peer {
                                    info!("acquired session '{}' by: {:?}", client_id, state.peer);
                                    if let Err(err) = session_event_tx
                                        .send(SessionEvent::SessionTakenOver(state))
                                        .await
                                        .context("SessionTaken event")
                                    {
                                        warn!(cause=?err, "SessionTaken event sent error");
                                    }
                                }
                                break;
                            }
                        } else {
                            warn!("wrong modified session: {:?}", key);
                            unreachable!()
                        }
                    }
                    Event::Remove { key } => {
                        trace!("session removed: {:?}", key);
                        break;
                    }
                }
            }
        });
    }
}

impl SubscriptionsStore for SessionContext {
    fn get_subscription(
        &self,
        client_id: &str,
        topic_filter: &[u8],
    ) -> Result<Option<SubscriptionOptions>> {
        let key = SubscriptionKey {
            session: client_id.as_ref(),
            topic_filter,
        };
        self.subscriptions
            .get(bincode::serialize(&key)?)
            .map(|o| o.and_then(|value| SubscriptionOptions::try_from(value.as_ref()).ok()))
            .map_err(Error::msg)
    }

    fn store_subscription(
        &self,
        client_id: &str,
        topic_filter: &[u8],
        option: &SubscriptionOptions,
    ) -> Result<()> {
        let key = SubscriptionKey {
            session: client_id.as_ref(),
            topic_filter,
        };
        self.subscriptions
            .insert(bincode::serialize(&key)?, bincode::serialize(&option)?)
            .map(|_| ())
            .map_err(Error::msg)
    }

    fn remove_subscription(&self, client_id: &str, topic_filter: &[u8]) -> Result<bool> {
        let key = SubscriptionKey {
            session: client_id.as_ref(),
            topic_filter,
        };
        self.subscriptions
            .remove(bincode::serialize(&key)?)
            .map(|d| d.is_some())
            .map_err(Error::msg)
    }

    fn list_subscriptions(
        &self,
        client_id: &str,
    ) -> Result<Vec<(IVec, SubscriptionOptions)>, Error> {
        let key = SubscriptionKey {
            session: client_id.as_ref(),
            topic_filter: "".as_ref(),
        };
        let prefix = bincode::serialize(&key)?;
        let mut ret = vec![];

        for res in self.subscriptions.scan_prefix(prefix) {
            match res {
                Ok((key, value)) => ret.push((key, SubscriptionOptions::try_from(value.as_ref())?)),
                Err(err) => warn!(cause = ?err, "subscription scan error:"),
            }
        }

        Ok(ret)
    }

    fn clear_subscriptions(&self, client_id: &str) -> Result<usize> {
        let key = SubscriptionKey {
            session: client_id.as_ref(),
            topic_filter: "".as_ref(),
        };
        let mut batch = Batch::default();
        let prefix = bincode::serialize(&key)?;
        let mut ret = 0;

        for res in self.subscriptions.scan_prefix(prefix) {
            match res {
                Ok((key, _)) => {
                    batch.remove(key);
                    ret += 1;
                }
                Err(err) => warn!(cause = ?err, "subscription scan error:"),
            }
        }
        self.subscriptions
            .apply_batch(batch)
            .map(|_| ret)
            .map_err(Error::msg)
    }
}
