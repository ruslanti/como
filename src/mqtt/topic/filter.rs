//https://github.com/CJP10/globber/tree/master/src
pub(crate) enum Token<'a> {
    Root,
    Topic(&'a str),
    SingleLevel,
    MultiLevel,
    Dollar(&'a str),
}

pub(crate) struct TopicMatcher<'a> {
    tokens: Vec<Token<'a>>,
}

pub struct TopicFilter<'a> {
    matcher: TopicMatcher<'a>,
}

impl TopicMatcher {
    pub(crate) fn new(tokens: Vec<Token>) -> Self {
        Self { tokens }
    }

    pub(crate) fn matches(&self, input: Chars) -> bool {
        match_index(&self.tokens, 0, input) == Status::Match
    }
}

impl TopicFilter {
    pub fn new(pattern: &str) -> Result<Self> {
        pattern.parse()
    }

    pub fn matches(&self, input: &str) -> bool {
        self.matcher.matches(input.chars())
    }
}
