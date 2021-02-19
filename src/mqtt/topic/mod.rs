use std::fmt;
use std::fmt::Debug;
use std::path::Path;
use std::time::Instant;

use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sled::{Db, IVec, Subscriber, Tree};
use tokio::sync::{watch, RwLock};
use tracing::{debug, instrument, trace, warn};

use path::TopicPath;

use crate::mqtt::proto::property::PublishProperties;
use crate::mqtt::proto::types::QoS;
use std::convert::{TryFrom, TryInto};

mod path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TopicMessage {
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
    nodes: RwLock<TopicPath<Topic>>,
}

impl Topics {
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
        Ok(Self {
            db,
            nodes: RwLock::new(nodes),
        })
    }

    #[instrument(skip(self, msg))]
    pub(crate) async fn publish(&self, name: &str, msg: TopicMessage) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        let topic = nodes.get(name)?;

        if let None = *topic {
            //new topic event
            debug!("new topic {}", name);
            //let id = tx.create_segment(topic_name)?;
            let log = self.db.open_tree(name)?;
            *topic = Some(Topic {
                name: name.to_owned(),
                log,
            });

            if topic.is_some() {
                /*if let Err(err) = self.new_topic.0.send(name.to_owned()) {
                    warn!(cause = ?err, "new topic event error");
                }*/
            }
        } else {
            debug!("found topic {}", name);
        };

        if let Some(topic) = topic {
            let id = self.db.generate_id()?;
            let value = bincode::serialize(&msg)?;
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

impl Drop for Topics {
    fn drop(&mut self) {
        if let Err(err) = self.db.flush() {
            eprintln!("topics db flush error: {}", err);
        }
    }
}

impl TryFrom<IVec> for TopicMessage {
    type Error = Error;

    fn try_from(encoded: IVec) -> Result<Self> {
        bincode::deserialize(encoded.as_ref()).map_err(Error::msg)
    }
}

impl fmt::Debug for Topics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Topics")
    }
}
