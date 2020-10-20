use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use futures::SinkExt;
use tokio::fs;
use tokio::sync::broadcast::RecvError::Lagged;
use tokio::sync::{broadcast, watch};
use tracing::{debug, error, instrument, trace, warn};

use crate::mqtt::proto::types::{MqttString, QoS};

#[derive(Debug, Clone)]
pub(crate) struct Message {
    pub retain: bool,
    pub qos: QoS,
    pub topic_name: MqttString,
    pub content_type: Option<Bytes>,
    pub data: Bytes,
}

type PublishTopic = broadcast::Sender<TopicEvent>;
pub(crate) type SubscribeTopic = broadcast::Receiver<TopicEvent>;
type RetainEvent = Option<Message>;
type RetainReceiver = watch::Receiver<RetainEvent>;
type RetainSender = watch::Sender<RetainEvent>;

#[derive(Debug, Clone)]
pub(crate) enum TopicEvent {
    NewTopic(String, PublishTopic),
    Publish(Message),
}

#[derive(Debug)]
pub struct Topic {
    publish_channel: PublishTopic,
    topics: HashMap<String, Self>,
    retain_rx: RetainReceiver,
}

#[derive(Debug, Clone, Copy)]
enum MatchState<'a> {
    Topic(&'a str),
    SingleLevel,
    MultiLevel,
}

impl Topic {
    pub(crate) fn new(name: String) -> Self {
        let (publish_channel, rx) = broadcast::channel(1024);
        let (retain_tx, retain_rx) = watch::channel(None);
        debug!("new topic");
        tokio::spawn(async move {
            Self::topic(name, rx, retain_tx).await;
        });
        Self {
            publish_channel,
            topics: HashMap::new(),
            retain_rx,
        }
    }

    pub(crate) fn subscribe_channel(&self) -> SubscribeTopic {
        self.publish_channel.subscribe()
    }

    pub(crate) fn new_topic_channel(&self) -> PublishTopic {
        self.publish_channel.clone()
    }

    #[instrument(skip(rx))]
    async fn topic(name: String, mut rx: SubscribeTopic, mut retain_tx: RetainSender) {
        trace!("new topic spawn start {:?}", rx);
        loop {
            match rx.recv().await {
                Ok(TopicEvent::Publish(msg)) => {
                    trace!("{:?}", msg);
                    if let Err(err) = retain_tx.broadcast(Some(msg)) {
                        error!(cause = ?err, "topic retain error: ");
                    }
                }
                Ok(t) => warn!("{:?}", t),
                Err(Lagged(lag)) => {
                    warn!("lagged: {}", lag);
                }
                Err(err) => {
                    error!(cause = ?err, "topic error: ");
                    break;
                }
            }
        }
        trace!("new topic spawn stop {:?}", rx);
    }

    #[instrument(skip(self, handler))]
    pub(crate) fn publish_topic<F>(&mut self, topic_name: &str, handler: F) -> PublishTopic
    where
        F: Fn(PublishTopic),
    {
        let mut s = topic_name.splitn(2, '/');
        match s.next() {
            Some(prefix) => {
                let topic = self.topics.entry(prefix.to_string()).or_insert_with(|| {
                    let topic = Topic::new(prefix.to_string());
                    handler(topic.publish_channel.clone());
                    topic
                });
                match s.next() {
                    Some(suffix) => topic.publish_topic(suffix, handler),
                    None => topic.publish_channel.clone(),
                }
            }
            None => self.publish_channel.clone(),
        }
    }

    #[instrument(skip(self))]
    pub(crate) fn subscribe_topic(
        &self,
        topic_filter: &str,
    ) -> Vec<(SubscribeTopic, RetainReceiver)> {
        let pattern = Topic::pattern(topic_filter);
        trace!("find path: {:?} => {:?}", topic_filter, pattern);
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
                                    node.publish_channel.subscribe(),
                                    node.retain_rx.clone(),
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
                                    node.publish_channel.subscribe(),
                                    node.retain_rx.clone(),
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
                            res.push((node.publish_channel.subscribe(), node.retain_rx.clone()));
                            for (node_name, node) in node.topics.iter() {
                                stack.push_back((level, node_name, node))
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
        for name in topic_name.split('/') {
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
                        if !name.starts_with("$") {
                            if (level + 1) == max_level {
                                return true;
                            }
                        }
                    }
                    MatchState::MultiLevel => {
                        //println!("pattern # - level {:?}", level);
                        if !name.starts_with("$") {
                            return true;
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
    use super::*;

    #[test]
    fn test_insert() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut root = Topic::new("".to_string());
            let handler = |_| ();
            println!("topic: {:?}", root.publish_topic("aaa/ddd", handler));
            println!("topic: {:?}", root.publish_topic("/aaa/bbb", handler));
            println!("topic: {:?}", root.publish_topic("/aaa/ccc", handler));
            println!("topic: {:?}", root.publish_topic("/aaa/ccc/", handler));
            println!("topic: {:?}", root.publish_topic("/aaa/bbb/ccc", handler));
            println!("topic: {:?}", root.publish_topic("ggg", handler));

            //root.find("aaa/ddd");
            println!("subscribe: {:?}", root.subscribe_topic("/aaa/#"));
        })
    }
}
