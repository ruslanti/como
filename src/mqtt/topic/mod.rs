use std::convert::TryFrom;
use std::fmt;
use std::fmt::Debug;
use std::path::Path;

use anyhow::{anyhow, Error, Result};
use serde::{Deserialize, Serialize};
use sled::{Db, Event, IVec, Subscriber, Tree};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tracing::{debug, instrument, trace, warn};

use path::TopicPath;

use crate::mqtt::proto::types::{Publish, QoS};

mod path;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PubMessage {
    pub retain: bool,
    pub qos: QoS,
    //pub properties: PublishProperties,
    pub payload: Vec<u8>,
}

struct Topic {
    name: String,
    log: Tree,
}

pub struct Topics<'a> {
    db: Db,
    nodes: RwLock<TopicPath<Topic>>,
    new_topic_event_tx: Sender<&'a str>,
    new_topic_event_rx: Receiver<&'a str>,
}

impl Topics<'static> {
    pub(crate) fn new(path: impl AsRef<Path> + Debug) -> Result<Self> {
        debug!("open topics db: {:?}", path);
        let db = sled::open(path)?;

        let mut nodes = TopicPath::new();

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
            nodes.get(name).map(|topic| {
                topic.get_or_insert(Topic {
                    name: name.to_string(),
                    log,
                })
            })?;
        }

        let (new_topic_event_tx, new_topic_event_rx) = mpsc::channel(32);

        Ok(Self {
            db,
            nodes: RwLock::new(nodes),
            new_topic_event_tx,
            new_topic_event_rx,
        })
    }

    pub(crate) fn topic_event(&self) -> Receiver<&'static str> {
        self.new_topic_event_rx.clone()
    }

    #[instrument(skip(self, msg))]
    pub(crate) async fn publish(&'static self, msg: Publish) -> Result<()> {
        let topic_name = std::str::from_utf8(&msg.topic_name[..])?;
        let mut nodes = self.nodes.write().await;
        let topic = nodes.get(topic_name)?;

        if let None = *topic {
            //new topic event
            debug!("new topic {}", topic_name);
            let log = self.db.open_tree(topic_name)?;
            *topic = Some(Topic {
                name: topic_name.to_owned(),
                log,
            });

            if let Some(topic) = topic {
                if let Err(err) = self.new_topic_event_tx.send(topic.name.as_str()).await {
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
    pub(crate) async fn subscribe(&self, topic_filter: &str) -> Result<Vec<(String, Subscriber)>> {
        Ok(self
            .nodes
            .read()
            .await
            .filter(topic_filter)?
            .into_iter()
            .map(|t| (t.name.to_owned(), t.log.watch_prefix(vec![])))
            .collect())
    }
    /*
    pub(crate) fn match_filter(topic_name: &str, filter: &str) -> bool {
        false
    }*/
}

impl Drop for Topics<'_> {
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

impl fmt::Debug for Topics<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Topics")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topicmessage_ser() {
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
