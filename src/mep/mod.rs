use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};

use anyhow::{anyhow, Result};
use tracing::{debug, instrument};

use crate::mep::subscription::Subscription;
use crate::mep::topic::{Publisher, Topic};

mod subscription;
mod topic;

enum Root<'a> {
    None,
    Dollar,
    Name(&'a str),
}

#[derive(Debug, Clone, Copy)]
enum MatchState<'a> {
    Topic(&'a str),
    SingleLevel,
    MultiLevel,
    Dollar,
}

struct Mep {
    topics: HashMap<String, Topic>,
}

impl Mep {
    fn handle_new_topic(name: String, publisher: Publisher) {
        debug!("new topic: {}", name);
    }

    fn get(&mut self, name: &str) -> Result<Publisher> {
        if let Some(topic_name) = name.strip_prefix('$') {
            let topic = self
                .topics
                .entry('$'.to_string())
                .or_insert_with(|| Topic::new("$"));
            topic.get(Root::Dollar, topic_name, Mep::handle_new_topic)
        } else {
            let mut s = name.splitn(2, '/');
            match s.next() {
                Some(prefix) => {
                    let path = Topic::path(Root::None, prefix);
                    let topic = self.topics.entry(prefix.to_owned()).or_insert_with(|| {
                        let topic = Topic::new(path.as_str());
                        Mep::handle_new_topic(path.to_owned(), Publisher::from(topic.borrow()));
                        topic
                    });
                    match s.next() {
                        Some(suffix) => {
                            topic.get(Root::Name(path.as_str()), suffix, Mep::handle_new_topic)
                        }
                        None => Ok(Publisher::from(topic.borrow())),
                    }
                }
                None => Err(anyhow!("invalid topic: {}", name)),
            }
        }
    }

    #[instrument(skip(self))]
    pub(crate) fn subscribe<'a>(&self, topic_filter: &str) -> Vec<Subscription> {
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
                                res.push(Subscription::from(node));
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
                                res.push(Subscription::from(node));
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
                            res.push(Subscription::from(node));
                            for (node_name, node) in node.siblings.iter() {
                                stack.push_back((level, node_name, node))
                            }
                        }
                    }
                    MatchState::Dollar => {
                        if name == "$" {
                            if (level + 1) == max_level {
                                // FINAL
                                res.push(Subscription::from(node));
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
}
