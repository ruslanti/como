use std::str::FromStr;

use anyhow::{Error, Result};

use crate::topic::parser::{parse_topic_filter, MatchToken, Token};
use crate::topic::path::TopicName;

#[derive(Debug, Eq, PartialEq)]
pub enum Status {
    Match,
    PartialMatch,
    NoMatch,
}

#[derive(Debug)]
pub struct TopicFilter {
    matcher: TopicMatcher,
}

#[derive(Debug)]
struct TopicMatcher {
    tokens: Vec<MatchToken>,
}

impl TopicFilter {
    pub fn matches(&self, topic_name: TopicName) -> Status {
        self.matcher.matches(topic_name)
    }
}

impl FromStr for TopicFilter {
    type Err = Error;

    fn from_str(s: &str) -> Result<TopicFilter, Self::Err> {
        Ok(Self {
            matcher: TopicMatcher::new(parse_topic_filter(s)?),
        })
    }
}

impl TopicMatcher {
    fn new(tokens: Vec<MatchToken>) -> Self {
        Self { tokens }
    }

    fn matches(&self, topic_name: TopicName) -> Status {
        let mut match_index = 0;
        for token in topic_name.tokens {
            let match_token = self.tokens.get(match_index);
            match (token, match_token) {
                (topic_token, Some(MatchToken::Token(match_token)))
                    if topic_token == *match_token =>
                {
                    match_index += 1
                }
                (Token::Root, Some(MatchToken::MultiLevel)) => return Status::Match,
                (Token::Topic(_), Some(MatchToken::SingleLevel)) => match_index += 1,
                (Token::Topic(_), Some(MatchToken::MultiLevel)) => return Status::Match,
                _ => return Status::NoMatch,
            }
        }

        if let Some(MatchToken::MultiLevel) = self.tokens.get(match_index) {
            Status::Match
        } else {
            match self.tokens.len() {
                l if l == match_index => Status::Match,
                l if l > match_index => Status::PartialMatch,
                _ => Status::NoMatch,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_match() {
        let topic_filter: TopicFilter = "".parse().unwrap();
        assert_eq!(Status::Match, topic_filter.matches("".parse().unwrap()));
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("test".parse().unwrap()),
        );

        let topic_filter: TopicFilter = "topic".parse().unwrap();
        assert_eq!(
            Status::Match,
            topic_filter.matches("topic".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("test".parse().unwrap()),
        );

        let topic_filter: TopicFilter = "/topic".parse().unwrap();
        assert_eq!(
            Status::Match,
            topic_filter.matches("/topic".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("topic".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("/test".parse().unwrap()),
        );

        let topic_filter: TopicFilter = "/topic/test".parse().unwrap();
        assert_eq!(
            Status::Match,
            topic_filter.matches("/topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("/test/test".parse().unwrap()),
        );

        let topic_filter: TopicFilter = "$topic/test".parse().unwrap();
        assert_eq!(
            Status::Match,
            topic_filter.matches("$topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("$test/test".parse().unwrap()),
        );
    }

    #[test]
    fn test_topic_single_match() {
        let topic_filter: TopicFilter = "+".parse().unwrap();
        assert_eq!(
            Status::PartialMatch,
            topic_filter.matches("".parse().unwrap())
        );
        assert_eq!(Status::Match, topic_filter.matches("test".parse().unwrap()));
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("$test".parse().unwrap()),
        );

        let topic_filter: TopicFilter = "/+".parse().unwrap();
        assert_eq!(
            Status::Match,
            topic_filter.matches("/topic".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("/topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("topic".parse().unwrap()),
        );
        assert_eq!(
            Status::Match,
            topic_filter.matches("/test".parse().unwrap()),
        );

        let topic_filter: TopicFilter = "/topic/+".parse().unwrap();
        assert_eq!(
            Status::Match,
            topic_filter.matches("/topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("/topic/test/test2".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("/test/test".parse().unwrap()),
        );

        let topic_filter: TopicFilter = "/+/+".parse().unwrap();
        assert_eq!(
            Status::Match,
            topic_filter.matches("/topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::PartialMatch,
            topic_filter.matches("/topic".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::Match,
            topic_filter.matches("/test/test".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("$topic/test".parse().unwrap()),
        );

        let topic_filter: TopicFilter = "$topic/+".parse().unwrap();
        assert_eq!(
            Status::Match,
            topic_filter.matches("$topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("$test/test".parse().unwrap()),
        );
    }

    #[test]
    fn test_topic_multi_match() {
        let topic_filter: TopicFilter = "#".parse().unwrap();
        assert_eq!(Status::Match, topic_filter.matches("".parse().unwrap()));
        assert_eq!(Status::Match, topic_filter.matches("test".parse().unwrap()));
        assert_eq!(
            Status::Match,
            topic_filter.matches("/test".parse().unwrap())
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("$test".parse().unwrap()),
        );
        assert_eq!(
            Status::Match,
            topic_filter.matches("topic/test".parse().unwrap()),
        );

        let topic_filter: TopicFilter = "/#".parse().unwrap();
        assert_eq!(
            Status::Match,
            topic_filter.matches("/topic".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("topic".parse().unwrap()),
        );
        assert_eq!(
            Status::Match,
            topic_filter.matches("/test".parse().unwrap()),
        );

        let topic_filter: TopicFilter = "topic/#".parse().unwrap();
        assert_eq!(
            Status::Match,
            topic_filter.matches("topic".parse().unwrap()),
        );

        let topic_filter: TopicFilter = "/topic/#".parse().unwrap();
        assert_eq!(
            Status::Match,
            topic_filter.matches("/topic".parse().unwrap()),
        );
        assert_eq!(
            Status::Match,
            topic_filter.matches("/topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::Match,
            topic_filter.matches("/topic/test/test2".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("/test/test".parse().unwrap()),
        );

        let topic_filter: TopicFilter = "+/topic02/#".parse().unwrap();
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("/topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::Match,
            topic_filter.matches("topic/topic02/".parse().unwrap()),
        );
        assert_eq!(
            Status::Match,
            topic_filter.matches("topic/topic02".parse().unwrap()),
        );
        assert_eq!(
            Status::Match,
            topic_filter.matches("topic/topic02/test2".parse().unwrap()),
        );
        assert_eq!(
            Status::Match,
            topic_filter.matches("topic/topic02/test2/test03".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("/test/test".parse().unwrap()),
        );

        let topic_filter: TopicFilter = "/#".parse().unwrap();
        assert_eq!(
            Status::Match,
            topic_filter.matches("/topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::Match,
            topic_filter.matches("/test/test".parse().unwrap()),
        );

        let topic_filter: TopicFilter = "$topic/#".parse().unwrap();
        assert_eq!(
            Status::Match,
            topic_filter.matches("$topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::Match,
            topic_filter.matches("$topic/test/02".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("$test/test".parse().unwrap()),
        );

        let topic_filter: TopicFilter = "$topic/#".parse().unwrap();
        assert_eq!(
            Status::Match,
            topic_filter.matches("$topic/test".parse().unwrap()),
        );
        assert_eq!(
            Status::Match,
            topic_filter.matches("$topic/test/02".parse().unwrap()),
        );
        assert_eq!(
            Status::NoMatch,
            topic_filter.matches("$test/test".parse().unwrap()),
        );
    }
}
