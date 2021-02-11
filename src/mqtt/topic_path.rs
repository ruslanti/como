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

impl RootType {
    fn path(&self) -> PathBuf {
        match self {
            RootType::RootDir => PathBuf::from("/"),
            RootType::Name(name) => PathBuf::from(name),
            RootType::Dollar(name) => PathBuf::from("$".to_owned() + name),
        }
    }
}

impl<T> TopicNode<T> {
    fn get(&mut self, topic_name: impl AsRef<Path>) -> Result<&mut Option<T>> {
        let mut components = topic_name.as_ref().components();
        if let Some(component) = components.next() {
            trace!("component: {:?}", component);
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
            trace!("component: {:?}", component);
            let key = Self::root_key(component.borrow())?;
            trace!("root_key: {:?}", key);
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
    }

    #[test]
    fn test_topic_path_err() {
        let mut topics: TopicPath<TestTopic> = TopicPath::new();
        assert!(topics.get("/aaa/$/").is_err());
    }
}
