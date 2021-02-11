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
use tokio::sync::broadcast::error::RecvError::Lagged;
use tokio::sync::{broadcast, watch};
use tracing::{debug, error, instrument, trace, warn};

use crate::mqtt::proto::property::PublishProperties;
use crate::mqtt::proto::types::QoS;

#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq)]
enum RootType {
    RootDir,
    Dollar(String),
    Name(String),
}

struct TopicNode<T> {
    topic: Option<T>,
    nodes: BTreeMap<String, Self>,
}

pub struct TopicPath<T> {
    nodes: BTreeMap<RootType, TopicNode<T>>,
}

#[derive(Debug, Clone, Copy)]
enum MatchState<'a> {
    Topic(&'a str),
    SingleLevel,
    MultiLevel,
    Dollar,
}

impl<T> TopicNode<T> {
    fn get(&mut self, topic_name: impl AsRef<Path>) -> Result<&mut Option<T>> {
        let mut components = topic_name.as_ref().components();
        if let Some(component) = components.next() {
            //trace!("component: {:?}", component);
            if let Component::Normal(name) = component {
                let name = name.to_str().ok_or(anyhow!("invalid os str"))?;
                if name.strip_prefix('$').is_none() {
                    self.nodes
                        .entry(name.to_owned())
                        .or_insert_with(|| TopicNode {
                            topic: None,
                            nodes: Default::default(),
                        })
                        .get(components.as_path())
                } else {
                    Err(anyhow!("Invalid leading $ character: {}", name))
                }
            } else {
                Err(anyhow!("invalid os str"))
            }
        } else {
            Ok(self.topic.borrow_mut())
        }
    }
}

impl<T> TopicPath<T> {
    pub fn get(&mut self, topic_name: impl AsRef<Path>) -> Result<&mut Option<T>> {
        let mut components = topic_name.as_ref().components();
        if let Some(component) = components.next() {
            //trace!("component: {:?}", component);
            let key = Self::root_key(component.borrow())?;
            // trace!("root_key: {:?}", key);
            self.nodes
                .entry(key)
                .or_insert_with(|| {
                    let node = TopicNode {
                        topic: None,
                        nodes: Default::default(),
                    };
                    node
                })
                .get(components.as_path())
        } else {
            Err(anyhow!("empty topic name"))
        }
    }

    fn root_key(component: &Component) -> Result<RootType> {
        match component {
            Component::RootDir => Ok(RootType::RootDir),
            Component::Normal(name) => {
                let name = name.to_str().ok_or(anyhow!("invalid os str"))?;
                if let Some(name) = name.strip_prefix('$') {
                    Ok(RootType::Dollar(name.to_owned()))
                } else {
                    Ok(RootType::Name(name.to_owned()))
                }
            }
            _ => Err(anyhow!("unexpected component: {:?}", component)),
        }
    }

    pub fn new() -> Self {
        TopicPath {
            nodes: Default::default(),
        }
    }

    #[instrument(skip(self))]
    pub(crate) fn subscribe(&self, topic_filter: &str) -> Vec<&T> {
        let pattern = Self::pattern(topic_filter);
        let mut res = Vec::new();
        // let path = PathBuf::new();

        let mut stack = VecDeque::new();
        for (name, node) in self.nodes.iter() {
            let name = match name {
                RootType::RootDir => "",
                RootType::Name(name) => name,
                RootType::Dollar(name) => name,
            };
            stack.push_back((0, name, node))
        }
        let max_level = pattern.len();
        while let Some((level, name, node)) = stack.pop_front() {
            if let Some(&state) = pattern.get(level) {
                // println!("state {:?} level: {} len: {}", state, level, pattern.len());
                match state {
                    MatchState::Topic(pattern) => {
                        //  println!("pattern {:?} - name {:?}", pattern, name);
                        if name == pattern {
                            if (level + 1) == max_level {
                                // FINAL
                                if let Some(topic) = node.topic.borrow() {
                                    res.push(topic);
                                }
                            }

                            for (node_name, node) in node.nodes.iter() {
                                stack.push_back((level + 1, node_name.borrow(), node))
                            }
                        }
                    }
                    MatchState::SingleLevel => {
                        //  println!("pattern + - level {:?}", level);
                        if (level + 1) == max_level {
                            // FINAL
                            if let Some(topic) = node.topic.borrow() {
                                res.push(topic);
                            }
                        }

                        for (node_name, node) in node.nodes.iter() {
                            stack.push_back((level + 1, node_name, node))
                        }
                    }
                    MatchState::MultiLevel => {
                        //println!("pattern # - level {:?}", level);
                        //  println!("FOUND {}", name);
                        if let Some(topic) = node.topic.borrow() {
                            res.push(topic);
                        }
                        for (node_name, node) in node.nodes.iter() {
                            stack.push_back((level, node_name, node))
                        }
                    }
                    MatchState::Dollar => {
                        if (level + 1) == max_level {
                            // FINAL
                            if let Some(topic) = node.topic.borrow() {
                                res.push(topic);
                            }
                        }

                        for (node_name, node) in node.nodes.iter() {
                            stack.push_back((level + 1, node_name, node))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default, Eq, PartialEq, Clone, Copy)]
    struct TestTopic<'a> {
        test: &'a str,
    }

    #[test]
    fn test_topic_path() {
        let mut topics: TopicPath<TestTopic> = TopicPath::new();

        let aaa = TestTopic { test: "aaa" };
        let _aaa = TestTopic { test: "$aaa" };
        let bbb = TestTopic { test: "/aaa/bbb" };
        let ccc = TestTopic { test: "/aaa/ccc" };
        let ccc_ = TestTopic { test: "/aaa/ccc/" };
        let ddd = TestTopic { test: "aaa/ddd" };
        assert_eq!("aaa", topics.get("aaa").unwrap().get_or_insert(aaa).test);
        assert_eq!("aaa", topics.get("aaa").unwrap().get_or_insert(bbb).test);
        assert_eq!("$aaa", topics.get("$aaa").unwrap().get_or_insert(_aaa).test);
        assert_eq!(
            "/aaa/bbb",
            topics.get("/aaa/bbb").unwrap().get_or_insert(bbb).test
        );
        assert_eq!("aaa", topics.get("/aaa").unwrap().get_or_insert(aaa).test);
        assert_eq!(
            "aaa/ddd",
            topics.get("aaa/ddd").unwrap().get_or_insert(ddd).test
        );
        assert_eq!(
            "/aaa/ccc",
            topics.get("/aaa/ccc").unwrap().get_or_insert(ccc).test
        );
        assert_eq!(
            "/aaa/ccc",
            topics.get("/aaa/ccc/").unwrap().get_or_insert(ccc_).test
        );

        /*
        aaa
        $aaa
        aaa/ddd
        /aaa/bbb
        /aaa/ccc
        */
        /*        let g: Vec<&str> = topics.subscribe("/aaa/+").iter().map(|t| t.test).collect();
        println!("{:?}", g);*/
        assert_eq!(
            vec!["aaa", "aaa/ddd", "/aaa/bbb", "/aaa/ccc"],
            topics
                .subscribe("#")
                .iter()
                .map(|t| t.test)
                .collect::<Vec<&str>>()
        );

        assert_eq!(
            vec!["/aaa/bbb", "/aaa/ccc"],
            topics
                .subscribe("/aaa/+")
                .iter()
                .map(|t| t.test)
                .collect::<Vec<&str>>()
        )
    }

    #[test]
    fn test_topic_path_err() {
        let mut topics: TopicPath<TestTopic> = TopicPath::new();
        assert!(topics.get("/aaa/$/").is_err());
    }
}
