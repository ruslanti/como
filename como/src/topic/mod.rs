use std::convert::TryFrom;
use std::fmt;
use std::fmt::Debug;

use anyhow::{anyhow, Error, Result};
use serde::{Deserialize, Serialize};
use sled::{Db, IVec, Subscriber, Tree};
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::RwLock;
use tracing::{debug, instrument, trace, warn};

use como_mqtt::v5::types::{Publish, QoS};

use crate::settings;
use crate::topic::filter::{Status, TopicFilter};
use crate::topic::path::TopicNode;

mod filter;
mod parser;
mod path;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PubMessage {
    pub retain: bool,
    pub qos: QoS,
    //pub properties: PublishProperties,
    pub payload: Vec<u8>,
}

struct Topic {
    name: String,
    log: Tree,
}

pub struct Topics {
    db: Db,
    nodes: RwLock<TopicNode<Topic>>,
    new_topic_event: Sender<String>,
}

impl Topics {
    pub fn new(cfg: settings::Topics) -> Result<Self> {
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
                Ok(name) => Some(name),
                Err(err) => {
                    warn!(cause = ?err, "topic name error");
                    None
                }
            }
        }) {
            debug!("init topic: {:?}", name);
            let log = db.open_tree(name)?;
            //let name = name.borrow();
            nodes.get(name.parse()?).map(|topic| {
                topic.get_or_insert(Topic {
                    name: name.to_string(),
                    log,
                })
            })?;
        }

        let (new_topic_event, _) = broadcast::channel(32);

        Ok(Self {
            db,
            nodes: RwLock::new(nodes),
            new_topic_event,
        })
    }

    pub fn topic_event(&self) -> Receiver<String> {
        self.new_topic_event.subscribe()
    }

    #[instrument(skip(self, msg))]
    pub async fn publish(&self, msg: Publish) -> Result<()> {
        let topic_name = std::str::from_utf8(&msg.topic_name[..])?;
        let mut nodes = self.nodes.write().await;
        let topic = nodes.get(topic_name.parse()?)?;

        if let None = *topic {
            //new topic event
            debug!("new topic {}", topic_name);
            let log = self.db.open_tree(topic_name)?;
            *topic = Some(Topic {
                name: topic_name.to_owned(),
                log,
            });

            if let Some(topic) = topic {
                if let Err(err) = self.new_topic_event.send(topic.name.clone()) {
                    warn!(cause = ?err, "new topic event error");
                }
            }
        } else {
            debug!("found topic {}", topic_name);
        };

        if let Some(topic) = topic {
            let id = self.db.generate_id()?;
            let m: PubMessage = msg.into();
            let value = bincode::serialize(&m)?;
            trace!("append {} - {:?} bytes", id, value);
            topic.log.insert(id.to_be_bytes(), value)?;
            Ok(())
        } else {
            Err(anyhow!("error"))
        }
    }

    #[instrument(skip(self))]
    pub async fn subscribe(&self, topic_filter: &str) -> Result<Vec<(String, Subscriber)>> {
        let node = self.nodes.read().await;
        let topic_filter = topic_filter.parse()?;
        Ok(node
            .filter(topic_filter)
            .into_iter()
            .map(|t| (t.name.to_owned(), t.log.watch_prefix(vec![])))
            .collect())
    }

    pub fn match_filter(topic_name: &str, topic_filter: &str) -> Result<bool> {
        let topic_filter = topic_filter.parse::<TopicFilter>()?;
        let topic_name = topic_name.parse()?;
        Ok(topic_filter.matches(topic_name) == Status::Match)
    }
}

impl Drop for Topics {
    fn drop(&mut self) {
        if let Err(err) = self.db.flush() {
            eprintln!("topics db flush error: {}", err);
        }
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
    fn test_topicmessage() {
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