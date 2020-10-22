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

pub(crate) type NewTopicEvent = (String, TopicSender, TopicRetainReceiver);
type NewTopicSender = broadcast::Sender<NewTopicEvent>;
type NewTopicReceiver = broadcast::Receiver<NewTopicEvent>;

type TopicSender = broadcast::Sender<Message>;
pub(crate) type TopicReceiver = broadcast::Receiver<Message>;

type TopicRetainEvent = Option<Message>;
type TopicRetainReceiver = watch::Receiver<TopicRetainEvent>;
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
    retain_rx: TopicRetainReceiver,
    siblings: HashMap<String, Self>,
}

#[derive(Debug)]
pub struct TopicManager {
    topic_tx: NewTopicSender,
    retain_rx: TopicRetainReceiver,
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
        let (topic_tx, subscribe_channel) = broadcast::channel(1024);
        let (retain_sender, retain_rx) = watch::channel(None);
        tokio::spawn(async move {
            Self::handle(subscribe_channel, retain_sender).await;
        });
        Self {
            topic_tx,
            retain_rx,
            topics: HashMap::new(),
        }
    }

    #[instrument(skip(topic_rx, retain_sender))]
    async fn handle(mut topic_rx: NewTopicReceiver, retain_sender: TopicRetainSender) {
        trace!("topic manager spawn start");
        let mut first = true;
        loop {
            match topic_rx.recv().await {
                Ok((topic, topic_tx, retain_rx)) => trace!("new topic event {:?}", topic),
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
        F: Fn(TopicSender, TopicRetainReceiver),
    {
        if let Some(topic_name) = topic_name.strip_prefix('$') {
            let topic = self.topics.entry('$'.to_string()).or_insert_with(|| {
                let topic = Topic::new("$".to_owned());
                new_topic_fn(topic.topic_tx.clone(), topic.retain_rx.clone());
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
                        new_topic_fn(topic.topic_tx.clone(), topic.retain_rx.clone());
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
                                    node.retain_rx.clone(),
                                ));
                            }

                            for (node_name, node) in node.siblings.iter() {
                                stack.push_back((level + 1, node_name, node))
                            }
                        }
                    }
                    MatchState::SingleLevel => {
                        //  println!("pattern + - level {:?}", level);
                        if !name.starts_with("$") {
                            if (level + 1) == max_level {
                                // FINAL
                                res.push((
                                    node.path.to_owned(),
                                    node.topic_tx.subscribe(),
                                    node.retain_rx.clone(),
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
                        if !name.starts_with("$") {
                            //  println!("FOUND {}", name);
                            res.push((
                                node.path.to_owned(),
                                node.topic_tx.subscribe(),
                                node.retain_rx.clone(),
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
                                    node.retain_rx.clone(),
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
        trace!("find path: {:?} => {:?}", filter, pattern);
        let mut level = 0;
        let topics = if let Some(topic_name) = topic_name.strip_prefix('$') {
            let mut names = vec![];
            names.push("$");
            topic_name.split('/').for_each(|n| names.push(n));
            names
        } else {
            topic_name.split('/').collect()
        };
        trace!("topics: {:?}", topics);
        for name in topics {
            if let Some(&state) = pattern.get(level) {
                let max_level = pattern.len();
                // println!("state {:?} level: {} len: {}", state, level, pattern.len());
                match state {
                    MatchState::Topic(pattern) => {
                        //  println!("pattern {:?} - name {:?}", pattern, name);
                        if name == pattern {
                            if (level + 1) == max_level {
                                return true;
                            }
                        }
                    }
                    MatchState::SingleLevel => {
                        //  println!("pattern + - level {:?}", level);
                        if !name.starts_with('$') {
                            if (level + 1) == max_level {
                                return true;
                            }
                        } else {
                            return false;
                        }
                    }
                    MatchState::MultiLevel => {
                        //println!("pattern # - level {:?}", level);
                        return if !name.starts_with('$') { true } else { false };
                    }
                    MatchState::Dollar => {
                        if name == "$" {
                            if (level + 1) == max_level {
                                return true;
                            }
                        }
                    }
                }
            };
            level = level + 1;
        }
        false
    }
}

impl Topic {
    fn new(name: String) -> Self {
        let (publish_channel, subscribe_channel) = broadcast::channel(1024);
        let (retain_sender, retain_channel) = watch::channel(None);
        debug!("create new \"{}\"", name);
        let path = name.to_owned();
        tokio::spawn(async move {
            Self::handle(name, subscribe_channel, retain_sender).await;
        });
        Self {
            path,
            topic_tx: publish_channel,
            retain_rx: retain_channel,
            siblings: HashMap::new(),
        }
    }

    #[instrument(skip(self, parent, new_topic_handler))]
    fn publish<F>(
        &mut self,
        parent: Root,
        topic_name: &str,
        new_topic_handler: F,
    ) -> Result<TopicSender>
    where
        F: Fn(TopicSender, TopicRetainReceiver),
    {
        if topic_name.strip_prefix('$').is_none() {
            let mut s = topic_name.splitn(2, '/');
            match s.next() {
                Some(prefix) => {
                    let path = Topic::path(parent, prefix);
                    let topic = self.siblings.entry(prefix.to_owned()).or_insert_with(|| {
                        let topic = Topic::new(path.to_owned());
                        new_topic_handler(topic.topic_tx.clone(), topic.retain_rx.clone());
                        topic
                    });
                    match s.next() {
                        Some(suffix) => {
                            topic.publish(Root::Name(path.as_str()), suffix, new_topic_handler)
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

    #[instrument(skip(subscribe_channel, retain_sender))]
    async fn handle(
        topic_name: String,
        mut subscribe_channel: TopicReceiver,
        retain_sender: TopicRetainSender,
    ) {
        trace!("start");
        let mut first = true;
        loop {
            match subscribe_channel.recv().await {
                Ok(msg) => {
                    trace!("{:?}", msg);
                    if msg.retain || first {
                        if let Err(err) = retain_sender.send(Some(msg)) {
                            error!(cause = ?err, "topic retain error: ");
                        } else {
                            first = false;
                        }
                    }
                }
                //Ok(TopicEvent::NewTopic(topic, _, _)) => trace!("new topic event {:?}", topic),
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
            let handler = |_, _| ();
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
