use std::collections::HashMap;
use std::collections::VecDeque;
use std::time::Instant;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use tokio::sync::broadcast::error::RecvError::Lagged;
use tokio::sync::{broadcast, watch};
use tracing::{debug, error, instrument, trace, warn};

use crate::mqtt::proto::property::PublishProperties;
use crate::mqtt::proto::types::{MqttString, QoS};

#[derive(Debug, Clone)]
pub(crate) struct Message {
    pub ts: Instant,
    pub retain: bool,
    pub qos: QoS,
    pub topic_name: MqttString,
    pub properties: PublishProperties,
    pub payload: Bytes,
}

pub(crate) type NewTopicEvent = (String, TopicSender, Message);
type NewTopicSender = broadcast::Sender<NewTopicEvent>;
type NewTopicReceiver = broadcast::Receiver<NewTopicEvent>;

pub(crate) type TopicSender = broadcast::Sender<Message>;
pub(crate) type TopicReceiver = broadcast::Receiver<Message>;

type TopicRetainEvent = Option<Message>;
pub(crate) type TopicRetainReceiver = watch::Receiver<TopicRetainEvent>;
type TopicRetainSender = watch::Sender<TopicRetainEvent>;

enum Root<'a> {
    None,
    Dollar,
    Name(&'a str),
}

#[derive(Debug)]
struct Topic {
    path: String,
    topic_tx: TopicSender,
    retained: TopicRetainReceiver,
    siblings: HashMap<String, Self>,
}

#[derive(Debug)]
pub struct TopicManager {
    topic_tx: NewTopicSender,
    topics: HashMap<String, Topic>,
}

#[derive(Debug, Clone, Copy)]
enum MatchState<'a> {
    Topic(&'a str),
    SingleLevel,
    MultiLevel,
    Dollar,
}

impl TopicManager {
    pub(crate) fn new() -> Self {
        let (topic_tx, topic_rx) = broadcast::channel(1024);
        tokio::spawn(async move {
            Self::topic_manager(topic_rx).await;
        });
        Self {
            topic_tx,
            topics: HashMap::new(),
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

    #[instrument(skip(self, new_topic_fn))]
    pub(crate) fn publish<F>(&mut self, topic_name: &str, new_topic_fn: F) -> Result<TopicSender>
    where
        F: Fn(String, TopicSender),
    {
        if let Some(topic_name) = topic_name.strip_prefix('$') {
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
                    let path = Topic::path(Root::None, prefix);
                    let topic = self.topics.entry(prefix.to_owned()).or_insert_with(|| {
                        let topic = Topic::new(path.to_owned());
                        new_topic_fn(path.to_owned(), topic.topic_tx.clone());
                        topic
                    });
                    match s.next() {
                        Some(suffix) => {
                            topic.publish(Root::Name(path.as_str()), suffix, new_topic_fn)
                        }
                        None => Ok(topic.topic_tx.clone()),
                    }
                }
                None => Err(anyhow!("invalid topic: {}", topic_name)),
            }
        }
    }

    #[instrument(skip(self))]
    pub(crate) fn subscribe<'a>(
        &self,
        topic_filter: &str,
    ) -> Vec<(String, TopicReceiver, TopicRetainReceiver)> {
        let pattern = Self::pattern(topic_filter);
        let mut res = Vec::new();

        let mut stack = VecDeque::new();
        for (node_name, node) in self.topics.iter() {
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
                                res.push((
                                    node.path.to_owned(),
                                    node.topic_tx.subscribe(),
                                    node.retained.clone(),
                                ));
                            }

                            for (node_name, node) in node.siblings.iter() {
                                stack.push_back((level + 1, node_name, node))
                            }
                        }
                    }
                    MatchState::SingleLevel => {
                        //  println!("pattern + - level {:?}", level);
                        if !name.starts_with('$') {
                            if (level + 1) == max_level {
                                // FINAL
                                res.push((
                                    node.path.to_owned(),
                                    node.topic_tx.subscribe(),
                                    node.retained.clone(),
                                ));
                                // println!("FOUND {}", name)
                            }

                            for (node_name, node) in node.siblings.iter() {
                                stack.push_back((level + 1, node_name, node))
                            }
                        }
                    }
                    MatchState::MultiLevel => {
                        //println!("pattern # - level {:?}", level);
                        if !name.starts_with('$') {
                            //  println!("FOUND {}", name);
                            res.push((
                                node.path.to_owned(),
                                node.topic_tx.subscribe(),
                                node.retained.clone(),
                            ));
                            for (node_name, node) in node.siblings.iter() {
                                stack.push_back((level, node_name, node))
                            }
                        }
                    }
                    MatchState::Dollar => {
                        if name == "$" {
                            if (level + 1) == max_level {
                                // FINAL
                                res.push((
                                    node.path.to_owned(),
                                    node.topic_tx.subscribe(),
                                    node.retained.clone(),
                                ));
                            }

                            for (node_name, node) in node.siblings.iter() {
                                stack.push_back((level + 1, node_name, node))
                            }
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
    fn new(name: String) -> Self {
        let (topic_tx, topic_rx) = broadcast::channel(1024);
        let (retained_tx, retained_rx) = watch::channel(None);
        debug!("create new \"{}\"", name);
        let path = name.to_owned();
        tokio::spawn(async move {
            Self::topic(name, topic_rx, retained_tx).await;
        });
        Self {
            path,
            topic_tx,
            retained: retained_rx,
            siblings: HashMap::new(),
        }
    }

    #[instrument(skip(self, parent, new_topic_fn))]
    fn publish<F>(&mut self, parent: Root, topic_name: &str, new_topic_fn: F) -> Result<TopicSender>
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
                        Some(suffix) => {
                            topic.publish(Root::Name(path.as_str()), suffix, new_topic_fn)
                        }
                        None => Ok(topic.topic_tx.clone()),
                    }
                }
                None => Ok(self.topic_tx.clone()),
            }
        } else {
            Err(anyhow!("Invalid leading $ character"))
        }
    }

    fn path(parent: Root, prefix: &str) -> String {
        match parent {
            Root::Name(parent) => {
                let mut path = parent.to_string();
                path.push('/');
                path.push_str(prefix);
                path
            }
            Root::Dollar => {
                let mut path = "$".to_string();
                path.push_str(prefix);
                path
            }
            Root::None => prefix.to_string(),
        }
    }

    #[instrument(skip(rx, retained))]
    async fn topic(topic_name: String, mut rx: TopicReceiver, retained: TopicRetainSender) {
        trace!("start");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut root = TopicManager::new();
            let handler = |name, _| println!("new topic: {}", name);
            println!("topic: {:?}", root.publish("aaa/ddd", handler));
            println!("topic: {:?}", root.publish("/aaa/bbb", handler));
            println!("topic: {:?}", root.publish("/aaa/ccc", handler));
            println!("topic: {:?}", root.publish("/aaa/ccc/", handler));
            println!("topic: {:?}", root.publish("/aaa/bbb/ccc", handler));
            println!("topic: {:?}", root.publish("ggg", handler));

            //root.find("aaa/ddd");
            println!("subscribe: {:?}", root.subscribe("/aaa/#"));
        })
    }
}
