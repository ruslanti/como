use std::str::FromStr;

use anyhow::{anyhow, Error, Result};

use crate::mqtt::topic::parser::{parse, Token};

pub(crate) struct TopicMatcher<'a> {
    tokens: Vec<Token<'a>>,
}

pub struct TopicFilter<'a> {
    matcher: TopicMatcher<'a>,
}

impl<'a> TopicMatcher<'a> {
    pub(crate) fn new(tokens: Vec<Token<'a>>) -> Self {
        Self { tokens }
    }

    pub(crate) fn matches(&self, input: &str) -> bool {
        //match_index(&self.tokens, 0, input) == Status::Match
        false
    }
}

impl<'a> TopicFilter<'a> {
    pub fn new(pattern: &'a str) -> Result<TopicFilter<'a>> {
        pattern.parse()
    }

    pub fn matches(&self, input: &'a str) -> bool {
        self.matcher.matches(input)
    }
}

impl<'a> FromStr for TopicFilter<'a> {
    type Err = Error;

    fn from_str(s: &str) -> Result<TopicFilter<'a>, Self::Err> {
        let g = parse(s)?;
        Ok(Self {
            matcher: TopicMatcher::new(g),
        })
    }
}
