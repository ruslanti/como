use std::convert::TryFrom;
use std::fmt;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use anyhow::{anyhow, Error, Result};
use serde::{Deserialize, Serialize};
use sled::{Db, IVec, Iter, Subscriber, Tree};
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::RwLock;
use tracing::{debug, instrument, trace, warn};

use como_mqtt::v5::types::{Publish, QoS};

use crate::metric;
use crate::settings;
use crate::topic::filter::{Status, TopicFilter};
use crate::topic::path::TopicNode;
use byteorder::{BigEndian, ReadBytesExt};

mod filter;
mod parser;
mod path;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PubMessage {
    pub retain: bool,
    pub qos: QoS,
    pub payload: Vec<u8>,
}

type RetainedMessage = (u64, PubMessage);

struct Topic {
    name: String,
    retained: Option<u64>,
}

pub(crate) struct Topics {
    db: Db,
    nodes: RwLock<TopicNode<Topic>>,
    new_topic_event: Sender<String>,
}

pub(crate) struct TopicManager(Arc<Topics>);

pub struct Values {
    inner: Iter,
}

impl Deref for TopicManager {
    type Target = Topics;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TopicManager {
    pub fn new(cfg: settings::Topics) -> Result<Self> {
        Ok(Self(Arc::new(Topics::new(cfg)?)))
    }

    pub fn match_filter(topic_name: &str, topic_filter: &str) -> Result<bool> {
        let topic_filter = topic_filter.parse::<TopicFilter>()?;
        let topic_name = topic_name.parse()?;
        Ok(topic_filter.matches(topic_name) == Status::Match)
    }
}

pub struct NewTopicSubscriber(Receiver<String>);

impl Deref for NewTopicSubscriber {
    type Target = Receiver<String>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for NewTopicSubscriber {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Topics {
    fn new(cfg: settings::Topics) -> Result<Self> {
        debug!("open topics db: {:?}", cfg);
        let db = sled::Config::new().temporary(cfg.temporary);
        let db = if let Some(path) = cfg.db_path {
            db.path(path).open()?
        } else {
            db.open()?
        };
        let mut nodes = TopicNode::new();

        for name in db.tree_names().iter().filter_map(|tree_name| {
            match std::str::from_utf8(tree_name.as_ref()) {
                Ok(name) if name == "__sled__default" => None,
                Ok(name) => Some(name.trim_end_matches('/')),
                Err(err) => {
                    warn!(cause = ?err, "topic name error");
                    None
                }
            }
        }) {
            debug!("init topic: {:?}", name);

            //let log = db.open_tree(name)?;
            //TODO find last retained message
            nodes
                .get_or_insert(name.parse()?)
                .get_or_insert_with(|| Topic {
                    name: name.to_owned(),
                    retained: None,
                });
            metric::TOPICS_NUMBER.with_label_values(&[]).inc();
        }

        let (new_topic_event, _) = broadcast::channel(1024);

        Ok(Self {
            db,
            nodes: RwLock::new(nodes),
            new_topic_event,
        })
    }

    pub fn topic_event(&self) -> NewTopicSubscriber {
        NewTopicSubscriber(self.new_topic_event.subscribe())
    }

    #[instrument(skip(self, msg))]
    pub async fn publish(&self, msg: Publish) -> Result<()> {
        let topic_name = std::str::from_utf8(&msg.topic_name[..])?;

        let id = self.db.generate_id()?;

        let log = self.db.open_tree(topic_name)?;
        if log.is_empty() {
            let mut nodes = self.nodes.write().await;
            let topic = nodes.get_or_insert(topic_name.parse()?);

            if topic.is_none() {
                let topic = topic.get_or_insert_with(|| {
                    debug!("new topic {}", topic_name);
                    Topic {
                        name: topic_name.to_owned(),
                        retained: None,
                    }
                });

                metric::TOPICS_NUMBER.with_label_values(&[]).inc();

                if let Err(err) = self
                    .new_topic_event
                    .send(topic.name.to_owned())
                    .map_err(|_| anyhow!("send error"))
                {
                    warn!(cause = ?err, "new topic event error");
                };
            }
        }

        if msg.retain {
            let mut nodes = self.nodes.write().await;
            if let Some(topic) = nodes.get_mut(topic_name.parse()?) {
                topic.retained.replace(id);
            } else {
                unreachable!()
            }
        };

        let pub_msg: PubMessage = msg.into();
        let value = bincode::serialize(&pub_msg)?;
        trace!("append {} - {:?} bytes", id, value);
        log.insert(id.to_be_bytes(), value)?;
        metric::TOPICS_SIZE
            .with_label_values(&[])
            .set(self.db.size_on_disk().unwrap_or(0) as i64);
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn subscribe(
        &self,
        topic_filter: &str,
    ) -> Result<Vec<(String, Subscriber, Option<RetainedMessage>)>> {
        let node = self.nodes.read().await;
        let topic_filter = topic_filter.parse()?;
        Ok(node
            .filter(topic_filter)
            .into_iter()
            .filter_map(|topic| {
                if let Ok(log) = self.db.open_tree(topic.name.as_str()) {
                    let retained = Topics::get_retained_msg(topic.retained, &log);
                    let subscriber = log.watch_prefix(vec![]);
                    Some((topic.name.to_owned(), subscriber, retained))
                } else {
                    None
                }
            })
            .collect())
    }

    fn get_retained_msg(retained_id: Option<u64>, log: &Tree) -> Option<RetainedMessage> {
        if let Some(id) = retained_id {
            match log.get(id.to_be_bytes()) {
                Ok(Some(value)) => match bincode::deserialize::<PubMessage>(value.as_ref()) {
                    Ok(msg) => Some((id, msg)),
                    Err(err) => {
                        warn!(cause = ?err, "deserialization error");
                        None
                    }
                },
                Ok(None) => None,
                Err(err) => {
                    warn!(cause = ?err, "topic load retain error");
                    None
                }
            }
        } else {
            None
        }
    }

    pub fn values(&self, topic_name: &str) -> Result<Values> {
        Ok(Values {
            inner: self.db.open_tree(topic_name)?.iter(),
        })
    }

    pub fn subscriber(&self, topic_name: &str) -> Result<Subscriber> {
        let log = self.db.open_tree(topic_name)?;
        Ok(log.watch_prefix(vec![]))
    }
}

impl Drop for Topics {
    fn drop(&mut self) {
        if let Err(err) = self.db.flush() {
            eprintln!("topics db flush error: {}", err);
        }
    }
}

impl Iterator for Values {
    type Item = (u64, PubMessage);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().and_then(|item| {
            item.ok().and_then(|(key, value)| {
                key.as_ref().read_u64::<BigEndian>().ok().and_then(|id| {
                    bincode::deserialize::<PubMessage>(value.as_ref())
                        .ok()
                        .map(|msg| (id, msg))
                })
            })
        })
    }
}

impl TryFrom<IVec> for PubMessage {
    type Error = Error;

    fn try_from(encoded: IVec) -> Result<Self> {
        bincode::deserialize(encoded.as_ref()).map_err(Error::msg)
    }
}

impl From<Publish> for PubMessage {
    fn from(msg: Publish) -> Self {
        PubMessage {
            retain: msg.retain,
            qos: msg.qos,
            payload: msg.payload.to_vec(),
        }
    }
}

impl fmt::Debug for Topics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Topics")
    }
}

#[cfg(test)]
mod tests {
    use como_mqtt::v5::types::QoS;

    use super::*;

    #[test]
    fn test_topic_message() {
        let m = PubMessage {
            retain: false,
            qos: QoS::AtMostOnce,
            payload: vec![],
        };

        let data = bincode::serialize(&m).unwrap();
        let n = bincode::deserialize(data.as_ref()).unwrap();
        assert_eq!(m, n);
    }
}
