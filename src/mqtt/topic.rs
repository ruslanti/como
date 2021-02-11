use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, bail, Error, Result};
use bytes::Bytes;
use futures::TryFutureExt;
use persy::{Config, Persy, PersyId, SegmentId, ValueMode};
use tokio::sync::broadcast::error::RecvError::Lagged;
use tokio::sync::{broadcast, watch, RwLock};
use tracing::{debug, error, instrument, trace, warn};

use crate::mqtt::proto::property::PublishProperties;
use crate::mqtt::proto::types::QoS;
use crate::mqtt::topic_path::TopicPath;

#[derive(Debug, Clone)]
pub(crate) struct TopicMessage {
    pub ts: Instant,
    pub retain: bool,
    pub qos: QoS,
    pub properties: PublishProperties,
    pub payload: Bytes,
}

type NewTopicSender = watch::Sender<String>;
type NewTopicReceiver = watch::Receiver<String>;

type TopicSender = watch::Sender<Option<TopicMessage>>;
type TopicReceiver = watch::Receiver<Option<TopicMessage>>;

type TopicRetainEvent = Option<TopicMessage>;
pub(crate) type TopicRetainReceiver = watch::Receiver<TopicRetainEvent>;
type TopicRetainSender = watch::Sender<TopicRetainEvent>;

struct Topic {
    segment: SegmentId,
    sender: TopicSender,
    receiver: TopicReceiver,
}

pub struct Topics {
    log: Persy,
    new_topic: (NewTopicSender, NewTopicReceiver),
    nodes: RwLock<TopicPath<Topic>>,
}

impl Topics {
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().join("topics").with_extension("db");
        debug!("create {:?}", path);
        let log = match Persy::open_or_create_with(path.as_path(), Config::new(), |log| {
            // this closure is only called on database creation
            //let mut tx = log.begin()?;
            // tx.create_segment("segment")?;
            //tx.create_index::<u64, PersyId>("index", ValueMode::EXCLUSIVE)?;
            // tx.create_index::<u64, PersyId>("timestamp", ValueMode::CLUSTER)?;
            // let prepared = tx.prepare()?;
            // prepared.commit()?;
            debug!("Segment and Index successfully created");
            Ok(())
        }) {
            Ok(log) => log,
            Err(err) => panic!("open topic db error: {:?}", err),
        };

        let mut nodes = TopicPath::new();

        match log.list_segments() {
            Ok(segments) => {
                for (segment, id) in segments {
                    debug!("topic: {}:{}", segment, id);
                    nodes
                        .get(segment)
                        .map(|topic| topic.get_or_insert(Topic::new(id)))
                        .unwrap();
                }
            }
            Err(err) => warn!(cause = ?err, "list topic error"),
        }
        Self {
            log,
            new_topic: watch::channel("".to_owned()),
            nodes: RwLock::new(nodes),
        }
    }

    #[instrument(skip(self, msg))]
    pub(crate) async fn publish(&self, topic_name: &str, msg: TopicMessage) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        let topic = nodes.get(topic_name)?;

        //write to persy log
        let mut tx = self.log.begin()?;
        //tx.create_index::<u64, PersyId>("index", ValueMode::EXCLUSIVE)?;
        //tx.create_index::<u64, PersyId>("timestamp", ValueMode::CLUSTER)?;

        if let None = *topic {
            //new topic event
            debug!("new topic {}", topic_name);
            let id = tx.create_segment(topic_name)?;
            *topic = Some(Topic::new(id));

            if let Err(err) = self.new_topic.0.send(topic_name.to_owned()) {
                warn!(cause = ?err, "new topic event error");
            }
        } else {
            debug!("found topic {}", topic_name);
        };

        if let Some(topic) = topic {
            let id = tx.insert(topic.segment, &*msg.payload)?;
            trace!("inserted log id: {:?}", id);
            let prepared = tx.prepare()?;
            prepared.commit()?;
            topic
                .sender
                .send(Some(msg))
                .map_err(|e| anyhow!("topic send error: {:?}", e))
        } else {
            Err(anyhow!("error"))
        }
    }

    #[instrument(skip(self))]
    pub(crate) async fn subscribe<'a>(&self, topic_filter: &str) -> Vec<(PathBuf, TopicReceiver)> {
        let nodes = self.nodes.read().await;
        //let pattern = Self::pattern(topic_filter);
        let mut res = Vec::new();
        // let path = PathBuf::new();

        res
    }

    pub(crate) fn match_filter(topic_name: &str, filter: &str) -> bool {
        /*let pattern = Self::pattern(filter);
        trace!(
            "match filter: {:?} => {:?} to topic_name: {}",
            filter,
            pattern,
            topic_name
        );
        let topics = if let Some(topic_name) = topic_name.strip_prefix('$') {
            let mut names = vec![];
            names.push("$");
            topic_name.split('/').for_each(|n| names.push(n));
            names
        } else {
            topic_name.split('/').collect()
        };
        for (level, name) in topics.into_iter().enumerate() {
            if let Some(&state) = pattern.get(level) {
                let max_level = pattern.len();
                /*                trace!(
                                    "state {:?} level: {} max_level: {} len: {}",
                                    state,
                                    level,
                                    max_level,
                                    pattern.len()
                                );
                */
                match state {
                    MatchState::Topic(pattern) => {
                        //trace!("pattern {:?} - name {:?}", pattern, name);
                        if name == pattern && (level + 1) == max_level {
                            return true;
                        }
                    }
                    MatchState::SingleLevel => {
                        // trace!("pattern + - level {:?}", level);
                        if !name.starts_with('$') {
                            if (level + 1) == max_level {
                                return true;
                            }
                        } else {
                            return false;
                        }
                    }
                    MatchState::MultiLevel => {
                        // trace!("pattern # - level {:?}", level);
                        return !name.starts_with('$');
                    }
                    MatchState::Dollar => {
                        if name == "$" && (level + 1) == max_level {
                            return true;
                        }
                    }
                }
            };
        }*/
        false
    }
}

impl Topic {
    #[instrument]
    fn new(segment: SegmentId) -> Self {
        trace!("create topic from segment: {:?}", segment);
        let (sender, receiver) = watch::channel(None);
        Self {
            segment,
            sender,
            receiver,
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
    /*use super::*;

    #[tokio::test]
    async fn test_topic() {
        let mut root = TopicManager::new("./data");
        let handler = |name, _| println!("new topic: {}", name);
        println!("topic: {:?}", root.publish("aaa"));
        println!("topic: {:?}", root.publish("$aaa"));
        println!("topic: {:?}", root.publish("/aaa"));

        println!("topic: {:?}", root.publish("aaa/ddd"));
        println!("topic: {:?}", root.publish("/aaa/bbb"));
        println!("topic: {:?}", root.publish("/aaa/ccc"));
        println!("topic: {:?}", root.publish("/aaa/ccc/"));
        println!("topic: {:?}", root.publish("/aaa/bbb/ccc"));
        println!("topic: {:?}", root.publish("ggg"));

        //root.find("aaa/ddd");
        println!("subscribe: {:?}", root.subscribe("/aaa/#"));
    }*/
}
