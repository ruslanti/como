use std::borrow::{Borrow, BorrowMut};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;

use anyhow::{Error, Result};

use crate::mqtt::topic::filter::{Status, TopicFilter};
use crate::mqtt::topic::parser::{parse_topic_name, Token};
use std::str::FromStr;

macro_rules! node_stack_push {
    ($node:ident, $topic_name:ident, $stack:ident) => {
        for (token, node) in $node.nodes.iter() {
            let mut tokens = Vec::with_capacity($topic_name.tokens.len());
            tokens.extend_from_slice($topic_name.tokens.as_slice());
            tokens.push(token.to_owned());
            $stack.push_back((TopicName { tokens }, node))
        }
    };
}

#[derive(Debug)]
pub(crate) struct TopicNode<T> {
    topic: Option<T>,
    nodes: BTreeMap<Token, Self>,
}

#[derive(Debug, Clone)]
pub(crate) struct TopicName {
    pub(crate) tokens: Vec<Token>,
}

impl FromStr for TopicName {
    type Err = Error;

    fn from_str(topic_name: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            tokens: parse_topic_name(topic_name)?,
        })
    }
}

impl<T> TopicNode<T> {
    pub(crate) fn get(&mut self, topic_name: TopicName) -> Result<&mut Option<T>> {
        if let Some(token) = topic_name.tokens.first() {
            self.nodes
                .entry(token.to_owned())
                .or_insert_with(|| TopicNode {
                    topic: None,
                    nodes: Default::default(),
                })
                .get(TopicName {
                    tokens: topic_name.tokens[1..].to_vec(),
                })
        } else {
            Ok(self.topic.borrow_mut())
        }
    }

    pub(crate) fn filter(&self, topic_filter: TopicFilter) -> Vec<&T> {
        let mut filtered = vec![];
        let topic_name = TopicName { tokens: vec![] };
        let mut stack = VecDeque::new();

        for (token, node) in self.nodes.iter() {
            let mut tokens = topic_name.tokens.clone();
            tokens.push(token.to_owned());
            stack.push_back((TopicName { tokens }, node))
        }

        while let Some((topic_name, node)) = stack.pop_front() {
            match topic_filter.matches(topic_name.clone()) {
                Status::Match => {
                    if let Some(topic) = node.topic.borrow() {
                        filtered.push(topic);
                    }
                    node_stack_push!(node, topic_name, stack);
                }
                Status::PartialMatch => {
                    node_stack_push!(node, topic_name, stack);
                }
                Status::NoMatch => {}
            }
        }

        filtered
    }

    pub fn new() -> Self {
        TopicNode {
            topic: None,
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
        let mut topics: TopicNode<TestTopic> = TopicNode::new();

        let aaa = TestTopic { test: "aaa" };
        let aaa_ = TestTopic { test: "/aaa" };
        let _aaa = TestTopic { test: "$aaa" };
        let bbb = TestTopic { test: "/aaa/bbb" };
        let ccc = TestTopic { test: "/aaa/ccc" };
        let ccc_ = TestTopic { test: "/aaa/ccc/" };
        let ddd = TestTopic { test: "aaa/ddd" };
        assert_eq!(
            "aaa",
            topics
                .get("aaa".parse().unwrap())
                .unwrap()
                .get_or_insert(aaa)
                .test
        );
        assert_eq!(
            "aaa",
            topics
                .get("aaa".parse().unwrap())
                .unwrap()
                .get_or_insert(bbb)
                .test
        );
        assert_eq!(
            "$aaa",
            topics
                .get("$aaa".parse().unwrap())
                .unwrap()
                .get_or_insert(_aaa)
                .test
        );
        assert_eq!(
            "/aaa/bbb",
            topics
                .get("/aaa/bbb".parse().unwrap())
                .unwrap()
                .get_or_insert(bbb)
                .test
        );
        assert_eq!(
            "/aaa",
            topics
                .get("/aaa".parse().unwrap())
                .unwrap()
                .get_or_insert(aaa_)
                .test
        );
        assert_eq!(
            "aaa/ddd",
            topics
                .get("aaa/ddd".parse().unwrap())
                .unwrap()
                .get_or_insert(ddd)
                .test
        );
        assert_eq!(
            "/aaa/ccc",
            topics
                .get("/aaa/ccc".parse().unwrap())
                .unwrap()
                .get_or_insert(ccc)
                .test
        );
        assert_eq!(
            "/aaa/ccc",
            topics
                .get("/aaa/ccc/".parse().unwrap())
                .unwrap()
                .get_or_insert(ccc_)
                .test
        );

        //println!("{:?}", topics);
        /*
        $aaa
        aaa/ddd
        /aaa/bbb
        /aaa/ccc
        */
        assert_eq!(
            vec!["aaa", "/aaa", "aaa/ddd", "/aaa/bbb", "/aaa/ccc"],
            topics
                .filter("#".parse().unwrap())
                .iter()
                .map(|t| t.test)
                .collect::<Vec<&str>>()
        );

        assert_eq!(
            vec!["/aaa", "/aaa/bbb", "/aaa/ccc"],
            topics
                .filter("/#".parse().unwrap())
                .iter()
                .map(|t| t.test)
                .collect::<Vec<&str>>()
        );

        assert_eq!(
            vec!["/aaa/bbb", "/aaa/ccc"],
            topics
                .filter("/aaa/+".parse().unwrap())
                .iter()
                .map(|t| t.test)
                .collect::<Vec<&str>>()
        );

        assert_eq!(
            vec!["aaa/ddd"],
            topics
                .filter("aaa/+".parse().unwrap())
                .iter()
                .map(|t| t.test)
                .collect::<Vec<&str>>()
        );

        assert_eq!(
            vec!["$aaa"],
            topics
                .filter("$aaa".parse().unwrap())
                .iter()
                .map(|t| t.test)
                .collect::<Vec<&str>>()
        );

        assert!(topics
            .filter("ggg/#".parse().unwrap())
            .iter()
            .map(|t| t.test)
            .collect::<Vec<&str>>()
            .is_empty());

        assert_eq!(
            vec!["/aaa/ccc"],
            topics
                .filter("/+/ccc".parse().unwrap())
                .iter()
                .map(|t| t.test)
                .collect::<Vec<&str>>()
        );
    }
}
