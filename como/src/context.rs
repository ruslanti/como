use std::borrow::Borrow;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::Result;
use anyhow::{Context, Error};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sled::{Batch, Db, Event, IVec, Subscriber, Tree};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tracing::{debug, info, instrument, trace, warn};

use como_mqtt::v5::error::MqttError;
use como_mqtt::v5::string::MqttString;
use como_mqtt::v5::types::{Publish, QoS, ReasonCode, SubscriptionOptions};

use crate::session::{SessionEvent, SubscribedTopics, TopicMessage};
use crate::settings::Settings;
use crate::topic::{NewTopicSubscriber, TopicManager, Values};

pub(crate) trait SessionStore {
    fn get(&self, client_id: &str) -> Result<Option<SessionState>>;
    fn acquire(
        &self,
        unique_id: u64,
        client_id: &str,
        peer: SocketAddr,
        clean_start: bool,
    ) -> Result<Option<SessionState>>;
    fn update(&self, client_id: &str, session_state: SessionState) -> Result<()>;
    fn remove(&self, client_id: &str) -> Result<()>;
    fn start_monitor(
        &self,
        unique_id: u64,
        client_id: &str,
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
    pub unique_id: u64,
    pub peer: SocketAddr,
    pub expire: Option<i64>,
    pub last_topic_id: Option<u64>,
}

#[derive(Clone)]
pub struct SessionContext(Arc<SessionContextInner>);

pub struct SessionContextInner {
    settings: Arc<Settings>,
    topic_manager: TopicManager,
    db: Db,
    sessions: Tree,
    subscriptions: Tree,
}

impl SessionState {
    pub fn new(unique_id: u64, peer: SocketAddr) -> Self {
        SessionState {
            unique_id,
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

#[instrument(skip(option, subscriber, session_event_tx))]
async fn subscription(
    option: SubscriptionOptions,
    session_event_tx: Sender<SessionEvent>,
    topic_name: String,
    mut subscriber: Subscriber,
) -> Result<()> {
    //   tokio::pin!(stream);
    trace!("start");
    while let Some(event) = (&mut subscriber).await {
        //debug!("receive subscription event {:?}", event);
        match event {
            Event::Insert { key, value } => match key.as_ref().read_u64::<BigEndian>() {
                Ok(id) => {
                    //debug!("id: {}, value: {:?}", id, value);
                    let msg = bincode::deserialize(value.as_ref()).context("deserialize event")?;
                    session_event_tx
                        .send(SessionEvent::TopicMessage(TopicMessage::new(
                            id,
                            topic_name.to_owned(),
                            option.clone(),
                            msg,
                        )))
                        .await
                        .context("subscription send")?
                }
                Err(_) => warn!("invalid log id: {:#x?}", key),
            },
            Event::Remove { .. } => unreachable!(),
        }
    }

    trace!("end");
    Ok(())
}

pub(crate) fn subscribe_topic(
    client_id: String,
    topic_name: String,
    options: SubscriptionOptions,
    subscription_tx: Sender<SessionEvent>,
    subscriber: Subscriber,
) -> oneshot::Sender<()> {
    let (unsubscribe_tx, unsubscribe_rx) = oneshot::channel();
    tokio::spawn(async move {
        tokio::select! {
            Err(err) = subscription(options, subscription_tx, topic_name.to_owned(), subscriber)
            => {
                warn!(cause = ?err, "subscription {} error", topic_name);
            },
            _ = unsubscribe_rx => {
                debug!("{:?} unsubscribe {}", client_id, topic_name);
            }
        }
    });
    unsubscribe_tx
}

impl Deref for SessionContext {
    type Target = SessionContextInner;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SessionContext {
    pub fn new(settings: Arc<Settings>) -> Result<Self> {
        Ok(Self(Arc::new(SessionContextInner::new(settings)?)))
    }
    pub fn settings(&self) -> &Settings {
        self.0.settings.borrow()
    }

    pub fn watch_new_topic(&self) -> NewTopicSubscriber {
        self.0.topic_manager.topic_event()
    }
}

impl SessionContextInner {
    pub fn new(settings: Arc<Settings>) -> Result<Self> {
        let db = sled::Config::new().temporary(true).create_new(true);
        let db = if let Some(path) = settings.connection.db_path.borrow() {
            db.path(path).open()?
        } else {
            db.open()?
        };
        let sessions = db.open_tree("sessions")?;
        let subscriptions = db.open_tree("subscriptions")?;

        let topic_manager = TopicManager::new(settings.topics.clone())?;

        Ok(SessionContextInner {
            settings,
            topic_manager,
            db,
            sessions,
            subscriptions,
        })
    }

    pub fn generate_id(&self) -> u64 {
        self.db.generate_id().unwrap_or_else(|_| rand::random())
    }

    pub(crate) async fn publish(&self, msg: Publish) -> Result<()> {
        self.topic_manager.publish(msg).await
    }

    #[instrument(skip(self, options, session_event_tx), err)]
    pub(crate) async fn subscribe(
        &self,
        client_id: &str,
        topic_filter: &str,
        options: &SubscriptionOptions,
        session_event_tx: &Sender<SessionEvent>,
    ) -> Result<(ReasonCode, SubscribedTopics)> {
        let mut unsubscribes = HashMap::new();
        let channels = self.topic_manager.subscribe(topic_filter).await?;
        debug!("subscribe returns {} subscriptions", channels.len());
        let reason_code = match options.qos {
            QoS::AtMostOnce => ReasonCode::Success,
            QoS::AtLeastOnce => ReasonCode::GrantedQoS1,
            QoS::ExactlyOnce => ReasonCode::GrantedQoS2,
        };

        for (topic_name, subscriber, retained) in channels {
            if let Some((id, msg)) = retained {
                if msg.retain && !msg.payload.is_empty() {
                    if let Err(err) = session_event_tx
                        .send(SessionEvent::TopicMessage(TopicMessage::new(
                            id,
                            topic_name.to_owned(),
                            options.clone(),
                            msg,
                        )))
                        .await
                    {
                        warn!(cause = ?err, "subscription send");
                    }
                }
            }

            unsubscribes.insert(
                topic_name.to_owned(),
                subscribe_topic(
                    client_id.to_owned(),
                    topic_name.to_owned(),
                    options.to_owned(),
                    session_event_tx.clone(),
                    subscriber,
                ),
            );
        }

        Ok((reason_code, unsubscribes))
    }

    pub fn topic_values(&self, topic_name: &str) -> Result<Values> {
        self.topic_manager.values(topic_name)
    }

    pub fn topic_subscriber(&self, topic_name: &str) -> Result<Subscriber> {
        self.topic_manager.subscriber(topic_name)
    }

    pub fn auth(
        &self,
        username: Option<MqttString>,
        password: Option<Bytes>,
    ) -> Result<(), MqttError> {
        let username = if let Some(username) = username.borrow() {
            Some(
                std::str::from_utf8(username.as_ref())
                    .map_err(|_| MqttError::BadUserNameOrPassword)?,
            )
        } else {
            None
        };

        let password = password.as_ref().map(|password| password.as_ref());

        match (self.settings.allow_anonymous, username, password) {
            (true, None, None) => Ok(()),
            (true, None, Some(_)) => Err(MqttError::NotAuthorized),
            (false, None, _) => Err(MqttError::NotAuthorized),
            (_, Some(_username), Some(_password)) => Ok(()),
            (_, Some(_username), None) => Err(MqttError::BadUserNameOrPassword),
        }
    }
}

impl SessionStore for SessionContextInner {
    fn get(&self, client_id: &str) -> Result<Option<SessionState>, Error> {
        self.sessions
            .get(client_id)
            .map(|value| value.and_then(|encoded| SessionState::try_from(encoded).ok()))
            .map_err(Error::msg)
    }

    fn acquire(
        &self,
        unique_id: u64,
        client_id: &str,
        peer: SocketAddr,
        clean_start: bool,
    ) -> Result<Option<SessionState>> {
        let update_fn = |old: Option<&[u8]>| -> Option<Vec<u8>> {
            let session_state = match old {
                Some(encoded) => {
                    if let Ok(mut session_state) = SessionState::try_from(encoded) {
                        session_state.unique_id = unique_id;
                        session_state.peer = peer;
                        session_state.expire = None;
                        if clean_start {
                            session_state.last_topic_id = None;
                        }
                        session_state
                    } else {
                        SessionState::new(unique_id, peer)
                    }
                }
                None => SessionState::new(unique_id, peer),
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
        unique_id: u64,
        client_id: &str,
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
                                if unique_id != state.unique_id {
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
                            warn!(
                                "wrong modified session: {:?}",
                                std::str::from_utf8(key.as_ref())
                                    .unwrap_or_else(|_| client_id.as_str())
                            );
                            unreachable!()
                        }
                    }
                    Event::Remove { key } => {
                        trace!(
                            "session removed: {:?}",
                            std::str::from_utf8(key.as_ref())
                                .unwrap_or_else(|_| client_id.as_str())
                        );
                        break;
                    }
                }
            }
        });
    }
}

impl SubscriptionsStore for SessionContextInner {
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
