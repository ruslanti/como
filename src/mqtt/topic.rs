use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::ops::Deref;
use std::str::FromStr;
use std::time::Instant;

use anyhow::Error;
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

type PublishChannel = broadcast::Sender<TopicEvent>;
pub(crate) type SubscribeChannel = broadcast::Receiver<TopicEvent>;
type RetainEvent = Option<Message>;
type RetainReceiver = watch::Receiver<RetainEvent>;
type RetainSender = watch::Sender<RetainEvent>;

#[derive(Debug, Clone)]
pub(crate) enum TopicEvent {
    NewTopic(String, PublishChannel, RetainReceiver),
    Publish(Message),
}

#[derive(Debug)]
pub struct Topic {
    publish_channel: PublishChannel,
    retain_channel: RetainReceiver,
    topics: HashMap<String, Self>,
}

#[derive(Debug, Clone, Copy)]
enum MatchState<'a> {
    Topic(&'a str),
    SingleLevel,
    MultiLevel,
    Dollar,
}

impl Topic {
    pub(crate) fn new(name: String) -> Self {
        let (publish_channel, subscribe_channel) = broadcast::channel(1024);
        let (retain_sender, retain_channel) = watch::channel(None);
        debug!("new topic");
        tokio::spawn(async move {
            Self::topic(name, subscribe_channel, retain_sender).await;
        });
        Self {
            publish_channel,
            retain_channel,
            topics: HashMap::new(),
        }
    }

    pub(crate) fn subscribe_channel(&self) -> SubscribeChannel {
        self.publish_channel.subscribe()
    }

    pub(crate) fn new_topic_channel(&self) -> PublishChannel {
        self.publish_channel.clone()
    }

    #[instrument(skip(subscribe_channel, retain_sender))]
    async fn topic(
        name: String,
        mut subscribe_channel: SubscribeChannel,
        retain_sender: RetainSender,
    ) {
        trace!("new topic spawn start {}", name);
        let mut first = true;
        loop {
            match subscribe_channel.recv().await {
                Ok(TopicEvent::Publish(msg)) => {
                    trace!("{:?}", msg);
                    if msg.retain || first {
                        if let Err(err) = retain_sender.send(Some(msg)) {
                            error!(cause = ?err, "topic retain error: ");
                        } else {
                            first = false;
                        }
                    }
                }
                Ok(TopicEvent::NewTopic(topic, _, _)) => trace!("new topic event {:?}", topic),
                Err(Lagged(lag)) => {
                    warn!("lagged: {}", lag);
                }
                Err(err) => {
                    error!(cause = ?err, "topic error: ");
                    break;
                }
            }
        }
        trace!("new topic spawn stop {}", name);
    }

    #[instrument(skip(self, new_topic_handler))]
    pub(crate) fn publish_topic<F>(
        &mut self,
        topic_name: &str,
        new_topic_handler: F,
    ) -> PublishChannel
    where
        F: Fn(PublishChannel, RetainReceiver),
    {
        if let Some(topic_name) = topic_name.strip_prefix('$') {
            let topic = self.topics.entry('$'.to_string()).or_insert_with(|| {
                let topic = Topic::new('$'.to_string());
                new_topic_handler(topic.publish_channel.clone(), topic.retain_channel.clone());
                topic
            });
            topic.publish_topic(topic_name, new_topic_handler)
        } else {
            let mut s = topic_name.splitn(2, '/');
            match s.next() {
                Some(prefix) => {
                    println!("'{}'", prefix);
                    let topic = self.topics.entry(prefix.to_owned()).or_insert_with(|| {
                        let topic = Topic::new(prefix.to_owned());
                        new_topic_handler(
                            topic.publish_channel.clone(),
                            topic.retain_channel.clone(),
                        );
                        topic
                    });
                    match s.next() {
                        Some(suffix) => topic.publish_topic(suffix, new_topic_handler),
                        None => topic.publish_channel.clone(),
                    }
                }
                None => self.publish_channel.clone(),
            }
        }
    }

    #[instrument(skip(root))]
    pub(crate) fn subscribe<'a>(
        root: &Topic,
        topic_filter: &str,
    ) -> Vec<(String, SubscribeChannel, RetainReceiver)> {
        let pattern = Topic::pattern(topic_filter);
        //trace!("find path: {:?} => {:?}", topic_filter, pattern);
        let mut res = Vec::new();

        let mut stack = VecDeque::new();
        for (node_name, node) in root.topics.iter() {
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
                                    name.to_owned(),
                                    node.publish_channel.subscribe(),
                                    node.retain_channel.clone(),
                                ));
                            }

                            for (node_name, node) in node.topics.iter() {
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
                                    name.to_owned(),
                                    node.publish_channel.subscribe(),
                                    node.retain_channel.clone(),
                                ));
                                // println!("FOUND {}", name)
                            }

                            for (node_name, node) in node.topics.iter() {
                                stack.push_back((level + 1, node_name, node))
                            }
                        }
                    }
                    MatchState::MultiLevel => {
                        //println!("pattern # - level {:?}", level);
                        if !name.starts_with("$") {
                            //  println!("FOUND {}", name);
                            res.push((
                                name.to_owned(),
                                node.publish_channel.subscribe(),
                                node.retain_channel.clone(),
                            ));
                            for (node_name, node) in node.topics.iter() {
                                stack.push_back((level, node_name, node))
                            }
                        }
                    }
                    MatchState::Dollar => {
                        if name == "$" {
                            if (level + 1) == max_level {
                                // FINAL
                                res.push((
                                    name.to_owned(),
                                    node.publish_channel.subscribe(),
                                    node.retain_channel.clone(),
                                ));
                            }

                            for (node_name, node) in node.topics.iter() {
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
        let pattern = Topic::pattern(filter);
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

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;

    use super::*;

    #[test]
    fn test_insert() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut root = Topic::new("".to_string());
            let handler = |_, _| ();
            println!("topic: {:?}", root.publish_topic("aaa/ddd", handler));
            println!("topic: {:?}", root.publish_topic("/aaa/bbb", handler));
            println!("topic: {:?}", root.publish_topic("/aaa/ccc", handler));
            println!("topic: {:?}", root.publish_topic("/aaa/ccc/", handler));
            println!("topic: {:?}", root.publish_topic("/aaa/bbb/ccc", handler));
            println!("topic: {:?}", root.publish_topic("ggg", handler));

            //root.find("aaa/ddd");
            println!("subscribe: {:?}", Topic::subscribe(root.borrow(), "/aaa/#"));
        })
    }
}
