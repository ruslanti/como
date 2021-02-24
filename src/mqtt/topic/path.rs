use std::borrow::{Borrow, BorrowMut};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::path::{Component, Path};

use anyhow::{anyhow, Context, Result};
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while, take_while1};
use nom::character::is_alphanumeric;
use nom::combinator::opt;
use nom::multi::many0;
use nom::sequence::{terminated, tuple};
use nom::IResult;
use tracing::instrument;

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
        let mut pattern =
            parse_topic_filter(topic_filter.as_bytes()).context("parse topic filter")?;
        let mut filtered = Vec::new();
        // println!("{:?}", pattern);

        match pattern.pop_front() {
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
        }
        Ok(filtered)
    }
}

fn parse_root(input: &[u8]) -> IResult<&[u8], Option<MatchState>> {
    opt(tag("/"))(input).map(|(remaining, parsed)| (remaining, parsed.map(|_| MatchState::Root)))
}

fn parse_dollar(input: &[u8]) -> IResult<&[u8], Option<MatchState>> {
    let mut dollar = opt(tuple((tag("$"), take_while1(is_alphanumeric))));
    dollar(input).map(|(remaining, parsed)| {
        (
            remaining,
            parsed.map(|(pr, name)| match pr {
                b"$" => MatchState::Dollar(std::str::from_utf8(name).unwrap()),
                _ => unreachable!(),
            }),
        )
    })
}

fn parse_suffix(input: &[u8]) -> IResult<&[u8], Option<MatchState>> {
    let mut suffix = opt(alt((tag("#"), take_while1(is_alphanumeric))));
    suffix(input).map(|(remaining, parsed)| {
        (
            remaining,
            parsed.map(|e| match e {
                b"#" => MatchState::MultiLevel,
                _ => MatchState::Topic(std::str::from_utf8(e).unwrap()),
            }),
        )
    })
}

fn parse_tokens(input: &[u8]) -> IResult<&[u8], Vec<MatchState>> {
    let token = alt((tag("+"), take_while(is_alphanumeric)));
    let mut tokens = many0(terminated(token, tag("/")));
    tokens(input).map(|(remaining, parsed)| {
        (
            remaining,
            parsed
                .into_iter()
                .filter_map(|e| match e {
                    b"+" => Some(MatchState::SingleLevel),
                    v if !v.is_empty() => Some(MatchState::Topic(std::str::from_utf8(v).unwrap())),
                    _ => None,
                })
                .collect(),
        )
    })
}

pub(crate) fn parse_topic_filter(topic_filter: &[u8]) -> Result<VecDeque<MatchState>> {
    let mut pattern = VecDeque::new();

    let err = |_| {
        anyhow!(
            "could not parse:  {}",
            std::str::from_utf8(topic_filter).unwrap()
        )
    };

    let (topic_filter, root) = parse_root(topic_filter)
        .map_err(err)
        .context("parse root")?;
    let topic_filter = if let Some(root) = root {
        pattern.push_back(root);
        topic_filter
    } else {
        let (topic_filter, prefix) = parse_dollar(topic_filter)
            .map_err(err)
            .context("parse dollar")?;
        if let Some(prefix) = prefix {
            pattern.push_back(prefix);
        };
        topic_filter
    };

    let (topic_filter, prefix) = parse_tokens(topic_filter)
        .map_err(err)
        .context("parse token")?;
    pattern.extend(prefix);

    let (topic_filter, suffix) = parse_suffix(topic_filter)
        .map_err(err)
        .context("parse suffix")?;
    if let Some(suffix) = suffix {
        pattern.push_back(suffix)
    };

    if topic_filter.is_empty() {
        Ok(pattern)
    } else {
        Err(anyhow!(
            "topic filter {:?} could not be parsed",
            topic_filter
        ))
    }
}

fn parse_topic_name(topic_name: &[u8]) -> Result<VecDeque<NodeType>> {
    let mut pattern = VecDeque::new();

    if topic_name.is_empty() {
        Ok(pattern)
    } else {
        Err(anyhow!("topic name {:?} could not be parsed", topic_name))
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
    fn test_nom_parser() {
        assert_eq!(
            parse_topic_filter(b"topic/").unwrap(),
            [MatchState::Topic("topic")]
        );
        assert_eq!(
            parse_topic_filter(b"topic/Test/").unwrap(),
            [MatchState::Topic("topic"), MatchState::Topic("Test")]
        );
        assert_eq!(
            parse_topic_filter(b"topic/+/").unwrap(),
            [MatchState::Topic("topic"), MatchState::SingleLevel]
        );
        assert_eq!(
            parse_topic_filter(b"+/topic/").unwrap(),
            [MatchState::SingleLevel, MatchState::Topic("topic")]
        );
        assert_eq!(
            parse_topic_filter(b"+/+/").unwrap(),
            [MatchState::SingleLevel, MatchState::SingleLevel]
        );

        assert_eq!(
            parse_topic_filter(b"topic/#").unwrap(),
            [MatchState::Topic("topic"), MatchState::MultiLevel]
        );
        assert!(parse_topic_filter(b"topic/$ff").is_err());

        assert_eq!(
            parse_topic_filter(b"topic/Test/#").unwrap(),
            [
                MatchState::Topic("topic"),
                MatchState::Topic("Test"),
                MatchState::MultiLevel
            ]
        );
        assert_eq!(
            parse_topic_filter(b"topic/+/#").unwrap(),
            [
                MatchState::Topic("topic"),
                MatchState::SingleLevel,
                MatchState::MultiLevel
            ]
        );
        assert_eq!(
            parse_topic_filter(b"+/topic/#").unwrap(),
            [
                MatchState::SingleLevel,
                MatchState::Topic("topic"),
                MatchState::MultiLevel
            ]
        );
        assert_eq!(
            parse_topic_filter(b"+/+/#").unwrap(),
            [
                MatchState::SingleLevel,
                MatchState::SingleLevel,
                MatchState::MultiLevel
            ]
        );

        assert_eq!(
            parse_topic_filter(b"/topic/").unwrap(),
            [MatchState::Root, MatchState::Topic("topic")]
        );
        assert_eq!(
            parse_topic_filter(b"/topic/Test/").unwrap(),
            [
                MatchState::Root,
                MatchState::Topic("topic"),
                MatchState::Topic("Test")
            ]
        );
        assert_eq!(
            parse_topic_filter(b"/topic/+/").unwrap(),
            [
                MatchState::Root,
                MatchState::Topic("topic"),
                MatchState::SingleLevel
            ]
        );
        assert_eq!(
            parse_topic_filter(b"/+/topic/").unwrap(),
            [
                MatchState::Root,
                MatchState::SingleLevel,
                MatchState::Topic("topic")
            ]
        );
        assert_eq!(
            parse_topic_filter(b"/+/+/").unwrap(),
            [
                MatchState::Root,
                MatchState::SingleLevel,
                MatchState::SingleLevel
            ]
        );

        assert_eq!(
            parse_topic_filter(b"/topic/#").unwrap(),
            [
                MatchState::Root,
                MatchState::Topic("topic"),
                MatchState::MultiLevel
            ]
        );

        assert!(parse_topic_filter(b"/topic/#/topic").is_err());

        assert_eq!(
            parse_topic_filter(b"/topic/Test/#").unwrap(),
            [
                MatchState::Root,
                MatchState::Topic("topic"),
                MatchState::Topic("Test"),
                MatchState::MultiLevel
            ]
        );
        assert_eq!(
            parse_topic_filter(b"/topic/+/#").unwrap(),
            [
                MatchState::Root,
                MatchState::Topic("topic"),
                MatchState::SingleLevel,
                MatchState::MultiLevel
            ]
        );
        assert_eq!(
            parse_topic_filter(b"/+/topic/#").unwrap(),
            [
                MatchState::Root,
                MatchState::SingleLevel,
                MatchState::Topic("topic"),
                MatchState::MultiLevel
            ]
        );
        assert_eq!(
            parse_topic_filter(b"/+/+/#").unwrap(),
            [
                MatchState::Root,
                MatchState::SingleLevel,
                MatchState::SingleLevel,
                MatchState::MultiLevel
            ]
        );

        assert_eq!(
            parse_topic_filter(b"$topic/").unwrap(),
            [MatchState::Dollar("topic")]
        );
        assert_eq!(
            parse_topic_filter(b"$topic/Test/").unwrap(),
            [MatchState::Dollar("topic"), MatchState::Topic("Test")]
        );
        assert_eq!(
            parse_topic_filter(b"$topic/+/").unwrap(),
            [MatchState::Dollar("topic"), MatchState::SingleLevel]
        );
        assert!(parse_topic_filter(b"$+/topic/").is_err());
        assert!(parse_topic_filter(b"$+/+/").is_err());

        assert_eq!(
            parse_topic_filter(b"$topic/#").unwrap(),
            [MatchState::Dollar("topic"), MatchState::MultiLevel]
        );
        assert_eq!(
            parse_topic_filter(b"$topic/Test/#").unwrap(),
            [
                MatchState::Dollar("topic"),
                MatchState::Topic("Test"),
                MatchState::MultiLevel
            ]
        );
        assert_eq!(
            parse_topic_filter(b"$topic/+/#").unwrap(),
            [
                MatchState::Dollar("topic"),
                MatchState::SingleLevel,
                MatchState::MultiLevel
            ]
        );
        assert!(parse_topic_filter(b"$+/topic/#").is_err());
        assert!(parse_topic_filter(b"$+/+/#").is_err());
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
