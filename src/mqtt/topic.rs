use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use futures::TryFutureExt;
use persy::{Config, Persy, PersyId, ValueMode};
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

pub(crate) type NewTopicEvent = (String, TopicMessage);
type NewTopicSender = broadcast::Sender<NewTopicEvent>;
type NewTopicReceiver = broadcast::Receiver<NewTopicEvent>;

type TopicSender = watch::Sender<Option<TopicMessage>>;
type TopicReceiver = watch::Receiver<Option<TopicMessage>>;

type TopicRetainEvent = Option<TopicMessage>;
pub(crate) type TopicRetainReceiver = watch::Receiver<TopicRetainEvent>;
type TopicRetainSender = watch::Sender<TopicRetainEvent>;

/*#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq)]
enum RootType {
    None,
    Dollar(String),
    Name(String),
}*/

/*struct Topic {
    sender: TopicSender,
    receiver: TopicReceiver,
}*/

pub struct Topics {
    path: PathBuf,
    //topic_tx: NewTopicSender,
    topics: RwLock<HashMap<String, TopicSender>>,
    nodes: RwLock<TopicPath<TopicReceiver>>,
}

#[derive(Debug, Clone, Copy)]
enum MatchState<'a> {
    Topic(&'a str),
    SingleLevel,
    MultiLevel,
    Dollar,
}

impl Topics {
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        let (topic_tx, topic_rx) = broadcast::channel(1024);

        tokio::spawn(async move {
            Self::topics(topic_rx).await;
        });

        Self {
            path: path.as_ref().to_owned(),
            topics: RwLock::new(HashMap::new()),
            nodes: RwLock::new(TopicPath::new()),
        }
    }

    pub(crate) async fn load(&self) -> Result<()> {
        Ok(())
    }

    #[instrument(skip(topic_rx))]
    async fn topics(mut topic_rx: NewTopicReceiver) {
        trace!("topic manager spawn start");
        loop {
            match topic_rx.recv().await {
                Ok((topic, _)) => trace!("new topic event {:?}", topic),
                Err(Lagged(lag)) => {
                    warn!("lagged: {}", lag);
                }
                Err(err) => {
                    error!(cause = ?err, "topic error: ");
                    break;
                }
            }
        }
        trace!("topic manager spawn stop");
    }

    #[instrument(skip(self, msg))]
    pub(crate) async fn publish(&self, topic_name: &str, msg: TopicMessage) -> Result<()> {
        let mut topics = self.topics.write().await;
        if !topics.contains_key(topic_name) {
            let (sender, receiver) = watch::channel(None);
            topics.insert(topic_name.to_owned(), sender);
            let mut nodes = self.nodes.write().await;
            nodes.get(topic_name)?.get_or_insert(receiver);
        };
        let topics = topics.downgrade();
        topics
            .get(topic_name)
            .unwrap()
            .send(Some(msg))
            .map_err(|e| anyhow!("topic send error: {:?}", e))
        /*        let sender = topics.get(topic_name);

                if let None = *sender {
                    debug!("new topic {}", topic_name);
                    let (tx, rx) = watch::channel(None);
                    let mut nodes = self.nodes.write().await;
                    nodes.get(topic_name)?.get_or_insert(rx);
                    *sender = Some(tx);
                }

                match sender {
                    Some(sender) => sender
                        .send(Some(msg))
                        .map_err(|e| anyhow!("topic send error: {:?}", e)),
                    None => unreachable!(),
                }
        */
        /* let mut nodes = self.nodes.write().await;
        let topic = nodes.get(topic_name)?;
        //TODO write to persy log
        if let None = *topic {
            debug!("new topic {}", topic_name);
            *topic = Some(Topic::new());
        //TODO new topic event
        } else {
            debug!("found topic {}", topic_name);
        };
        match topic {
            Some(topic) => topic
                .sender
                .send(Some(msg))
                .map_err(|e| anyhow!("topic send error: {:?}", e)),
            None => unreachable!(),
        }*/
    }

    #[instrument(skip(self))]
    pub(crate) fn subscribe<'a>(
        &self,
        topic_filter: &str,
    ) -> Vec<(PathBuf, TopicReceiver, TopicRetainReceiver)> {
        let pattern = Self::pattern(topic_filter);
        let mut res = Vec::new();
        /* let path = PathBuf::new();

        let mut stack = VecDeque::new();
        for (node_name, node) in self.nodes.iter() {
            stack.push_back((0, node_name, node))
        }

        while let Some((level, name, node)) = stack.pop_front() {
            if let Some(&state) = pattern.get(level) {
                let max_level = pattern.len();
                // println!("state {:?} level: {} len: {}", state, level, pattern.len());
                match state {
                    MatchState::Topic(pattern) => {
                        //  println!("pattern {:?} - name {:?}", pattern, name);
                        if name == pattern {
                            if (level + 1) == max_level {
                                // FINAL
                                /*                                res.push((
                                                                    node.path.to_owned(),
                                                                    node.topic_tx.subscribe(),
                                                                    node.retained.clone(),
                                                                ));
                                */
                            }

                            for (node_name, node) in node.siblings.iter() {
                                /*stack.push_back((level + 1, RootType::Name(node_name).borrow(), node))*/
                            }
                        }
                    }
                    MatchState::SingleLevel => {
                        //  println!("pattern + - level {:?}", level);
                        if (level + 1) == max_level {
                            // FINAL
                            /*                            res.push((
                                node.path.to_owned(),
                                node.topic_tx.subscribe(),
                                node.retained.clone(),
                            ));*/
                            // println!("FOUND {}", name)
                        }

                        for (node_name, node) in node.siblings.iter() {
                            /*stack.push_back((level + 1, RootType::Name(node_name), node))*/
                        }
                    }
                    MatchState::MultiLevel => {
                        //println!("pattern # - level {:?}", level);
                        //  println!("FOUND {}", name);
                        /*                        res.push((
                            node.path.to_owned(),
                            node.topic_tx.subscribe(),
                            node.retained.clone(),
                        ));*/
                        for (node_name, node) in node.siblings.iter() {
                            /*stack.push_back((level, RootType::Name(node_name), node))*/
                        }
                    }
                    MatchState::Dollar => {
                        if (level + 1) == max_level {
                            // FINAL
                            /*                            res.push((
                                node.path.to_owned(),
                                node.topic_tx.subscribe(),
                                node.retained.clone(),
                            ));*/
                        }

                        for (node_name, node) in node.siblings.iter() {
                            /*stack.push_back((level + 1, RootType::Name(node_name), node))*/
                        }
                    }
                }
            }
        }*/
        res
    }

    fn pattern(topic_filter: &str) -> Vec<MatchState> {
        let mut pattern = Vec::new();
        let topic_filter = if let Some(topic_filter) = topic_filter.strip_prefix('$') {
            pattern.push(MatchState::Dollar);
            topic_filter
        } else {
            topic_filter
        };

        for item in topic_filter.split('/') {
            match item {
                "#" => pattern.push(MatchState::MultiLevel),
                "+" => pattern.push(MatchState::SingleLevel),
                _ => pattern.push(MatchState::Topic(item)),
            }
        }
        pattern
    }

    pub(crate) fn match_filter(topic_name: &str, filter: &str) -> bool {
        let pattern = Self::pattern(filter);
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
        }
        false
    }
}

/*impl Topic {
    #[instrument]
    fn new() -> Self {
        let (sender, receiver) = watch::channel(None);
        /*        tokio::spawn(async move {
            if let Err(err) = Self::topic(name, receiver).await {
                warn!(cause = ?err, "topic error");
            }
        });*/

        Self { sender, receiver }
    }

    /* #[instrument(skip(self, parent, new_topic_fn))]
    fn publish<F>(
        &mut self,
        parent: RootType,
        topic_name: &str,
        new_topic_fn: F,
    ) -> Result<TopicSender>
    where
        F: Fn(String, TopicSender),
    {
        if topic_name.strip_prefix('$').is_none() {
            let mut s = topic_name.splitn(2, '/');
            match s.next() {
                Some(prefix) => {
                    let path = Topic::path(parent, prefix);
                    let topic = self.siblings.entry(prefix.to_owned()).or_insert_with(|| {
                        let topic = Topic::new(path.to_owned());
                        new_topic_fn(path.to_owned(), topic.topic_tx.clone());
                        topic
                    });
                    match s.next() {
                        Some(suffix) => topic.publish(RootType::Name(path), suffix, new_topic_fn),
                        None => Ok(topic.topic_tx.clone()),
                    }
                }
                None => Ok(self.topic_tx.clone()),
            }
        } else {
            Err(anyhow!("Invalid leading $ character"))
        }
    }*/

    /*    fn path(parent: RootType, name: &str) -> String {
        match parent {
            RootType::Name(parent) => {
                let mut path = parent.to_string();
                path.push('/');
                path.push_str(name);
                path
            }
            RootType::Dollar(_) => {
                let mut path = "$".to_string();
                path.push_str(name);
                path
            }
            RootType::None => name.to_string(),
        }
    }*/

    /*#[instrument(skip(receiver))]
    async fn topic(name: String, mut receiver: TopicReceiver) -> Result<()> {
        trace!("start");

        /*if let Err(e) = tokio::fs::read_dir(path.as_ref()).await {
            debug!("create dir");
            tokio::fs::create_dir(path.as_ref())
                .await
                .map_err(Error::msg)?;
        }

        let log_path = path.as_ref().join("data").with_extension("db");
        let log = Persy::open_or_create_with(log_path.as_path(), Config::new(), |log| {
            // this closure is only called on database creation
            let mut tx = log.begin()?;
            tx.create_segment("segment")?;
            tx.create_index::<u64, PersyId>("index", ValueMode::EXCLUSIVE)?;
            tx.create_index::<u64, PersyId>("timestamp", ValueMode::CLUSTER)?;
            let prepared = tx.prepare()?;
            prepared.commit()?;
            debug!("Segment and Index successfully created");
            Ok(())
        })
        .unwrap();*/

        loop {
            /* match rx.recv().await {
                Ok(msg) => {
                    // trace!("{:?}", msg);
                    if msg.retain && !msg.payload.is_empty() {
                        if let Err(err) = retained.send(Some(msg)) {
                            error!(cause = ?err, "topic retain error: ");
                        }
                    }
                }
                Err(Lagged(lag)) => {
                    warn!("lagged: {}", lag);
                }
                Err(err) => {
                    error!(cause = ?err, "topic error: ");
                    break;
                }
            }*/
        }
        trace!("stop");
        Ok(())
    }*/
}*/

impl fmt::Debug for Topics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.path.display())
    }
}

/*impl fmt::Debug for Topic {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}
*/

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
