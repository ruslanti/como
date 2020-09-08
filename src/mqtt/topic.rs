use std::collections::HashMap;
use std::collections::VecDeque;

use bytes::Bytes;
use tokio::sync::broadcast::RecvError::Lagged;
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, error, trace, warn};

#[derive(Debug, Clone)]
pub(crate) struct Message {
    pub retain: bool,
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
    pub(crate) fn new() -> Self {
        let (channel, mut rx) = broadcast::channel(32);
        debug!("new topic channel {:?}", channel);
        tokio::spawn(async move {
            debug!("new topic spawn start {:?}", rx);
            loop {
                match rx.recv().await {
                    Ok(msg) => debug!("received: {:?}", msg),
                    Err(Lagged(lag)) => {
                        warn!("lagged: {}", lag);
                    }
                    Err(err) => {
                        error!(cause = ?err, "topic error: ");
                        break;
                    }
                }
            }
            debug!("new topic spawn stop {:?}", rx);
        });
        Topic {
            channel,
            topics: Default::default(),
        }
    }

    pub(crate) fn publish(&mut self, path: &str) -> PublishTopic {
        println!("get {:?}", path);
        let mut s = path.splitn(2, '/');
        match s.next() {
            Some(prefix) => {
                let topic = self
                    .topics
                    .entry(prefix.to_string())
                    .or_insert_with(|| Topic::new());
                match s.next() {
                    Some(suffix) => topic.publish(suffix),
                    None => topic.channel.clone(),
                }
            }
            None => self.channel.clone(),
        }
    }

    pub(crate) fn subscribe(&self, path: &str) -> Vec<SubscribeTopic> {
        let mut pattern = Vec::new();
        for item in path.split('/') {
            match item {
                "#" => pattern.push(MatchState::MultiLevel),
                "+" => pattern.push(MatchState::SingleLevel),
                _ => pattern.push(MatchState::Topic(item)),
            }
        }
        println!("find path: {:?} => {:?}", path, pattern);
        let mut res = Vec::new();

        let mut stack = VecDeque::new();
        for (node_name, node) in self.topics.iter() {
            stack.push_back((0, node_name, node))
        }

        while let Some((level, name, node)) = stack.pop_front() {
            if let Some(&state) = pattern.get(level) {
                let max_level = pattern.len();
                println!("state {:?} level: {} len: {}", state, level, pattern.len());
                match state {
                    MatchState::Topic(pattern) => {
                        println!("pattern {:?} - name {:?}", pattern, name);
                        if name == pattern {
                            if (level + 1) == max_level {
                                // FINAL
                                res.push(node.channel.subscribe());
                                println!("FOUND {}", name)
                            }

                            for (node_name, node) in node.topics.iter() {
                                stack.push_back((level + 1, node_name, node))
                            }
                        }
                    }
                    MatchState::SingleLevel => {
                        println!("pattern + - level {:?}", level);
                        if !name.starts_with("$") {
                            if (level + 1) == max_level {
                                // FINAL
                                res.push(node.channel.subscribe());
                                println!("FOUND {}", name)
                            }

                            for (node_name, node) in node.topics.iter() {
                                stack.push_back((level + 1, node_name, node))
                            }
                        }
                    }
                    MatchState::MultiLevel => {
                        println!("pattern # - level {:?}", level);
                        if !name.starts_with("$") {
                            println!("FOUND {}", name);
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
        let mut root = Topic::new();

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
