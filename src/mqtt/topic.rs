use std::borrow::{Borrow, BorrowMut};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::fmt::Debug;
use std::path::{Component, Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use futures::TryFutureExt;
use persy::{Config, Persy, PersyId, ValueMode};
use tokio::io::AsyncBufReadExt;
use tokio::sync::broadcast::error::RecvError::Lagged;
use tokio::sync::{broadcast, watch};
use tokio_stream::StreamExt;
use tracing::{debug, error, instrument, trace, warn};

use crate::mqtt::proto::property::PublishProperties;
use crate::mqtt::proto::types::QoS;

#[derive(Debug, Clone)]
pub(crate) struct TopicMessage {
    pub ts: Instant,
    pub retain: bool,
    pub qos: QoS,
    pub properties: PublishProperties,
    pub payload: Bytes,
}

pub(crate) type NewTopicEvent = (String, TopicSender, TopicMessage);
type NewTopicSender = broadcast::Sender<NewTopicEvent>;
type NewTopicReceiver = broadcast::Receiver<NewTopicEvent>;

pub(crate) type TopicSender = broadcast::Sender<TopicMessage>;
pub(crate) type TopicReceiver = broadcast::Receiver<TopicMessage>;

type TopicRetainEvent = Option<TopicMessage>;
pub(crate) type TopicRetainReceiver = watch::Receiver<TopicRetainEvent>;
type TopicRetainSender = watch::Sender<TopicRetainEvent>;

#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq)]
enum RootType {
    None,
    Dollar(String),
    Name(String),
}

struct Topic {
    topic_tx: TopicSender,
    retained: TopicRetainReceiver,
}

struct Node {
    topic: Topic,
    siblings: BTreeMap<String, Self>,
}

type NodesCollection = BTreeMap<String, Node>;

pub struct TopicManager {
    path: PathBuf,
    topic_tx: NewTopicSender,
    topics: HashMap<String, TopicSender>,
    root_nodes: NodesCollection,
    dollar_nodes: NodesCollection,
    nodes: NodesCollection,
}

#[derive(Debug, Clone, Copy)]
enum MatchState<'a> {
    Topic(&'a str),
    SingleLevel,
    MultiLevel,
    Dollar,
}

impl Node {
    fn get(&mut self, path: impl AsRef<Path>, topic_name: impl AsRef<Path>) -> Result<&Topic> {
        let mut components = topic_name.as_ref().components();
        if let Some(component) = components.next() {
            if let Component::Normal(name) = component {
                let name = name.to_str().ok_or(anyhow!("invalid os str"))?;
                if name.strip_prefix('$').is_none() {
                    let path = path.as_ref().to_path_buf().join(name);
                    let node = if self.siblings.contains_key(name) {
                        self.siblings.get_mut(name).unwrap()
                    } else {
                        let topic = Topic::load(path.join(name))?;
                        self.siblings.entry(name.to_owned()).or_insert(Node {
                            topic,
                            siblings: Default::default(),
                        })
                    };
                    node.get(path, components.as_path().to_path_buf())
                } else {
                    Err(anyhow!("Invalid leading $ character: {}", name))
                }
            } else {
                Err(anyhow!("invalid os str"))
            }
        } else {
            Ok(self.topic.borrow())
        }
    }
}

impl TopicManager {
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        let (topic_tx, topic_rx) = broadcast::channel(1024);

        tokio::spawn(async move {
            Self::topic_manager(topic_rx).await;
        });

        Self {
            path: path.as_ref().to_owned(),
            topic_tx,
            topics: Default::default(),

            root_nodes: Default::default(),
            dollar_nodes: Default::default(),
            nodes: Default::default(),
        }
    }

    pub(crate) async fn load(&mut self) -> Result<()> {
        TopicManager::load_inner(self.path.join("01"), self.root_nodes.borrow_mut()).await?;
        TopicManager::load_inner(self.path.join("02"), self.dollar_nodes.borrow_mut()).await?;
        TopicManager::load_inner(self.path.join("03"), self.nodes.borrow_mut()).await?;
        /*
                let mut metadata = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(self.path.join("topics.db"))
                    .await?;
                // read topic metadata file
                let buf = BufReader::new(metadata.borrow_mut());
                let mut lines = buf.lines();
                while let Some(line) = lines.next_line().await? {
                    let err = Err(anyhow!("invalid topic meta entry: {}", line));
                    let mut splits = line.split_ascii_whitespace();
                    let (topic_name, log) = match splits.next() {
                        Some(topic_name) => match splits.next() {
                            Some(log) => (topic_name, self.path.join(log)),
                            None => return err,
                        },
                        None => return err,
                    };
                    let topic = Topic::new(log);
                    self.get(topic_name, topic.borrow())?;
                    if let Some(_) = self.topics.insert(topic_name.to_owned(), topic) {
                        return Err(anyhow!("duplicate topic: {:?}", topic_name));
                    }
                }

        */
        Ok(())
    }

    async fn load_inner(path: impl AsRef<Path>, nodes: &NodesCollection) -> Result<()> {
        if let Ok(mut entries) = tokio::fs::read_dir(path.as_ref()).await {
            while let Some(entry) = entries.next_entry().await? {
                if let Ok(file_type) = entry.file_type().await {
                    if file_type.is_dir() {
                        debug!(
                            "topic {:?}",
                            entry
                                .file_name()
                                .to_str()
                                .ok_or(anyhow!("invalid os str"))?
                        );
                    }
                } else {
                    warn!("Couldn't get file type for {:?}", entry.path());
                }
            }
        } else {
            trace!("create dir: {:?}", path.as_ref());
            tokio::fs::create_dir(path).map_err(Error::msg).await?;
        }
        Ok(())
    }

    async fn get(&mut self, topic_name: impl AsRef<Path>) -> Result<&Topic> {
        let mut components = topic_name.as_ref().components();
        if let Some(component) = components.next() {
            let (nodes, path, name) = match component {
                Component::RootDir => {
                    if let Some(component) = components.next() {
                        match component {
                            Component::Normal(name) => {
                                let name = name.to_str().ok_or(anyhow!("invalid os str"))?;
                                if name.strip_prefix('$').is_none() {
                                    (self.root_nodes.borrow_mut(), self.path.join("01"), name)
                                } else {
                                    return Err(anyhow!("Invalid leading $ character: {}", name));
                                }
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        return Err(anyhow!("empty root topic name"));
                    }
                }
                Component::Normal(name) => {
                    let name = name.to_str().ok_or(anyhow!("invalid os str"))?;
                    if let Some(name) = name.strip_prefix('$') {
                        (self.dollar_nodes.borrow_mut(), self.path.join("02"), name)
                    } else {
                        (self.nodes.borrow_mut(), self.path.join("03"), name)
                    }
                }
                _ => unreachable!(),
            };

            let node = if nodes.contains_key(name) {
                nodes.get_mut(name).unwrap()
            } else {
                let topic = Topic::load(path.join(name))?;
                nodes.entry(name.to_owned()).or_insert(Node {
                    topic,
                    siblings: Default::default(),
                })
            };
            node.get(path, components.as_path().to_path_buf())
        } else {
            Err(anyhow!("empty topic name"))
        }
    }

    #[instrument(skip(topic_rx))]
    async fn topic_manager(mut topic_rx: NewTopicReceiver) {
        trace!("topic manager spawn start");
        loop {
            match topic_rx.recv().await {
                Ok((topic, _, _)) => trace!("new topic event {:?}", topic),
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

    pub(crate) fn subscribe_channel(&self) -> NewTopicReceiver {
        self.topic_tx.subscribe()
    }

    pub(crate) fn new_topic_channel(&self) -> NewTopicSender {
        self.topic_tx.clone()
    }

    #[instrument(skip(self))]
    pub(crate) async fn publish(&mut self, topic_name: &str, msg: TopicMessage) -> Result<()> {
        let tx = if let Some(topic_tx) = self.topics.get(topic_name) {
            topic_tx.clone()
        } else {
            let topic = self.get(topic_name).await?;
            let topic_tx = topic.topic_tx.clone();
            self.topics.insert(topic_name.to_owned(), topic_tx.clone());
            topic_tx
        };
        tx.send(msg)
            .map_err(|e| anyhow!("{:?}", e))
            .map(|size| trace!("publish topic channel send {} message", size))

        /*if let Some(topic_name) = topic_name.strip_prefix('$') {
            let topic = self.topics.entry('$'.to_string()).or_insert_with(|| {
                let topic = Topic::new("$".to_owned());
                new_topic_fn("$".to_owned(), topic.topic_tx.clone());
                topic
            });
            topic.publish(Root::Dollar, topic_name, new_topic_fn)
        } else {
            let mut s = topic_name.splitn(2, '/');
            match s.next() {
                Some(prefix) => {
                    println!("SOME prefix '{}'", prefix);
                    let path = Topic::path(Root::None, prefix);
                    println!("PATH '{}'", path);
                    let topic = self.topics.entry(prefix.to_owned()).or_insert_with(|| {
                        let topic = Topic::new(path.to_owned());
                        new_topic_fn(path.to_owned(), topic.topic_tx.clone());
                        topic
                    });
                    match s.next() {
                        Some(suffix) => {
                            println!("suffix '{}'", suffix);
                            topic.publish(Root::Name(path.as_str()), suffix, new_topic_fn)
                        }
                        None => Ok(topic.topic_tx.clone()),
                    }
                }
                None => Err(anyhow!("invalid topic: {}", topic_name)),
            }
        }*/
    }

    #[instrument(skip(self))]
    pub(crate) fn subscribe<'a>(
        &self,
        topic_filter: &str,
    ) -> Vec<(PathBuf, TopicReceiver, TopicRetainReceiver)> {
        let pattern = Self::pattern(topic_filter);
        let mut res = Vec::new();
        let path = PathBuf::new();

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
        }
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

impl Topic {
    #[instrument]
    fn load(path: impl AsRef<Path> + Debug + Send + Sync + 'static) -> Result<Self> {
        let (topic_tx, topic_rx) = broadcast::channel(1024);
        let (retained_tx, retained_rx) = watch::channel(None);
        tokio::spawn(async move {
            Self::topic(path, topic_rx, retained_tx).await;
        });
        Ok(Self {
            topic_tx,
            retained: retained_rx,
        })
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

    fn path(parent: RootType, name: &str) -> String {
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
    }

    #[instrument(skip(rx, retained))]
    async fn topic(
        path: impl AsRef<Path> + Debug,
        mut rx: TopicReceiver,
        retained: TopicRetainSender,
    ) {
        trace!("start");
        debug!("create new {:?}", path.as_ref());
        tokio::fs::create_dir(path.as_ref()).await.unwrap();
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
        .unwrap();

        loop {
            match rx.recv().await {
                Ok(msg) => {
                    trace!("{:?}", msg);
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
            }
        }
        trace!("stop");
    }
}

impl fmt::Debug for TopicManager {
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
    use super::*;

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
    }
}
