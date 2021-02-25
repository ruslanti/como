use std::borrow::{Borrow, BorrowMut};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::path::{Component, Path};

use anyhow::{anyhow, Context, Result};
use tracing::instrument;

use crate::mqtt::topic::parser::parse;

#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq)]
enum NodeType {
    Root,
    Dollar(String),
    Name(String),
}

#[derive(Debug)]
struct TopicNode<T> {
    topic: Option<T>,
    nodes: BTreeMap<String, Self>,
}

#[derive(Debug)]
pub struct TopicPath<T> {
    nodes: BTreeMap<NodeType, TopicNode<T>>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum MatchState<'a> {
    Root,
    Topic(&'a str),
    SingleLevel,
    MultiLevel,
    Dollar(&'a str),
}

macro_rules! filter_result {
    ($node:ident, $pattern:ident, $matched:ident) => {
        if $pattern.is_empty() {
            if let Some(topic) = $node.topic.borrow() {
                $matched.push(topic);
            }
        } else {
            $matched.append($node.filter($pattern).as_mut());
        }
    };
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

    fn filter(&self, pattern: &mut VecDeque<MatchState>) -> Vec<&T> {
        let mut filtered = Vec::new();
        match pattern.pop_front() {
            Some(state) => {
                for (name, node) in self.nodes.iter() {
                    match state {
                        MatchState::Topic(token) if token == name => {
                            filter_result!(node, pattern, filtered);
                        }
                        MatchState::SingleLevel => {
                            filter_result!(node, pattern, filtered);
                        }
                        MatchState::MultiLevel => {
                            pattern.push_back(MatchState::MultiLevel);
                            if let Some(topic) = node.topic.borrow() {
                                filtered.push(topic);
                            }
                            filtered.append(node.filter(pattern).as_mut());
                        }
                        _ => {}
                    }
                }
            }
            None => {}
        }
        filtered
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

    fn root_key(component: &Component) -> Result<NodeType> {
        match component {
            Component::RootDir => Ok(NodeType::Root),
            Component::Normal(name) => {
                let name = name.to_str().ok_or(anyhow!("invalid os str"))?;
                if name.starts_with('$') {
                    Ok(NodeType::Dollar(name.to_owned()))
                } else {
                    Ok(NodeType::Name(name.to_owned()))
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
    pub(crate) fn filter(&self, topic_filter: &str) -> Result<Vec<&T>> {
        let mut pattern = parse(topic_filter).context("parse topic filter")?;
        let mut filtered = Vec::new();
        // println!("{:?}", pattern);

        /*match pattern.pop_front() {
            Some(state) => {
                for (root, node) in self.nodes.iter() {
                    match root {
                        NodeType::Root => match state {
                            MatchState::Root | MatchState::SingleLevel => {
                                let pattern = &mut pattern;
                                filter_result!(node, pattern, filtered);
                            }
                            MatchState::MultiLevel => {
                                let pattern = &mut pattern;
                                pattern.push_back(MatchState::MultiLevel);
                                filter_result!(node, pattern, filtered);
                            }
                            _ => {}
                        },
                        NodeType::Name(name) => match state {
                            MatchState::Topic(token) if token == name => {
                                let pattern = &mut pattern;
                                filter_result!(node, pattern, filtered);
                            }
                            MatchState::SingleLevel => {
                                let pattern = &mut pattern;
                                filter_result!(node, pattern, filtered);
                            }
                            MatchState::MultiLevel => {
                                pattern.push_back(MatchState::MultiLevel);
                                if let Some(topic) = node.topic.borrow() {
                                    filtered.push(topic);
                                }
                                filtered.append(node.filter(&mut pattern).as_mut());
                            }
                            _ => {}
                        },
                        NodeType::Dollar(name) if state == MatchState::Dollar(name) => {
                            let pattern = &mut pattern;
                            filter_result!(node, pattern, filtered);
                        }
                        _ => {}
                    }
                }
            }
            None => {}
        }*/
        Ok(filtered)
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
        let aaa_ = TestTopic { test: "/aaa" };
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
        assert_eq!("/aaa", topics.get("/aaa").unwrap().get_or_insert(aaa_).test);
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

        //println!("{:?}", topics);
        /*
        $aaa
        aaa/ddd
        /aaa/bbb
        /aaa/ccc
        */
        /*        let g: Vec<&str> = topics.subscribe("/aaa/+").iter().map(|t| t.test).collect();
        println!("{:?}", g);*/
        assert_eq!(
            vec!["/aaa", "/aaa/bbb", "/aaa/ccc", "aaa", "aaa/ddd"],
            topics
                .filter("#")
                .unwrap()
                .iter()
                .map(|t| t.test)
                .collect::<Vec<&str>>()
        );

        assert_eq!(
            vec!["/aaa", "/aaa/bbb", "/aaa/ccc"],
            topics
                .filter("/#")
                .unwrap()
                .iter()
                .map(|t| t.test)
                .collect::<Vec<&str>>()
        );

        assert_eq!(
            vec!["/aaa/bbb", "/aaa/ccc"],
            topics
                .filter("/aaa/+")
                .unwrap()
                .iter()
                .map(|t| t.test)
                .collect::<Vec<&str>>()
        );

        assert_eq!(
            vec!["aaa/ddd"],
            topics
                .filter("aaa/+")
                .unwrap()
                .iter()
                .map(|t| t.test)
                .collect::<Vec<&str>>()
        );

        assert_eq!(
            vec!["$aaa"],
            topics
                .filter("$aaa")
                .unwrap()
                .iter()
                .map(|t| t.test)
                .collect::<Vec<&str>>()
        );

        assert!(topics
            .filter("ggg/#")
            .unwrap()
            .iter()
            .map(|t| t.test)
            .collect::<Vec<&str>>()
            .is_empty());

        assert_eq!(
            vec!["/aaa/ccc"],
            topics
                .filter("/+/ccc")
                .unwrap()
                .iter()
                .map(|t| t.test)
                .collect::<Vec<&str>>()
        );
    }

    #[test]
    fn test_topic_path_err() {
        let mut topics: TopicPath<TestTopic> = TopicPath::new();
        assert!(topics.get("/aaa/$/").is_err());
    }
}
