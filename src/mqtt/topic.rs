use std::collections::HashMap;
use std::collections::VecDeque;

use bytes::Bytes;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::RecvError::Lagged;
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

type PublishTopic = broadcast::Sender<Message>;
pub(crate) type SubscribeTopic = broadcast::Receiver<Message>;

#[derive(Debug)]
pub struct Topic {
    channel: PublishTopic,
    topics: HashMap<String, Self>,
}

#[derive(Debug, Clone, Copy)]
enum MatchState<'a> {
    Topic(&'a str),
    SingleLevel,
    MultiLevel,
}

impl Topic {
    pub(crate) fn new(name: String) -> Self {
        let (channel, rx) = broadcast::channel(1024);
        debug!("new topic");
        tokio::spawn(async move {
            Topic::topic(name, rx).await;
        });
        Topic {
            channel,
            topics: Default::default(),
        }
    }

    #[instrument(skip(rx))]
    async fn topic(name: String, mut rx: Receiver<Message>) {
        trace!("new topic spawn start {:?}", rx);
        loop {
            match rx.recv().await {
                Ok(msg) => trace!("{:?}", msg),
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

    #[instrument(skip(self))]
    pub(crate) fn publish_topic(&mut self, topic_name: &str) -> PublishTopic {
        //  println!("get {:?}", topic_name);
        let mut s = topic_name.splitn(2, '/');
        match s.next() {
            Some(prefix) => {
                let topic = self
                    .topics
                    .entry(prefix.to_string())
                    .or_insert_with(|| Topic::new(prefix.to_string()));
                match s.next() {
                    Some(suffix) => topic.publish_topic(suffix),
                    None => topic.channel.clone(),
                }
            }
            None => self.channel.clone(),
        }
    }

    #[instrument(skip(self))]
    pub(crate) fn subscribe_topic(&self, topic_name: &str) -> Vec<SubscribeTopic> {
        let mut pattern = Vec::new();
        for item in topic_name.split('/') {
            match item {
                "#" => pattern.push(MatchState::MultiLevel),
                "+" => pattern.push(MatchState::SingleLevel),
                _ => pattern.push(MatchState::Topic(item)),
            }
        }
        //  println!("find path: {:?} => {:?}", topic_name, pattern);
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
                                res.push(node.channel.subscribe());
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
                                res.push(node.channel.subscribe());
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
                            res.push(node.channel.subscribe());
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert() {
        let mut root = Topic::new("".to_string());

        println!("topic: {:?}", root.publish_topic("aaa/ddd"));
        println!("topic: {:?}", root.publish_topic("/aaa/bbb"));
        println!("topic: {:?}", root.publish_topic("/aaa/ccc"));
        println!("topic: {:?}", root.publish_topic("/aaa/ccc/"));
        println!("topic: {:?}", root.publish_topic("/aaa/bbb/ccc"));
        println!("topic: {:?}", root.publish_topic("ggg"));

        //root.find("aaa/ddd");
        println!("subscribe: {:?}", root.subscribe_topic("/aaa/#"));
    }
}
