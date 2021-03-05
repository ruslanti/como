use anyhow::{anyhow, Context, Result};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alphanumeric0, alphanumeric1};
use nom::combinator::opt;
use nom::multi::many0;
use nom::sequence::{terminated, tuple};
use nom::IResult;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum Token {
    Root,
    Topic(Vec<u8>),
    Dollar(Vec<u8>),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MatchToken {
    Token(Token),
    SingleLevel,
    MultiLevel,
}

fn parse_root(input: &str) -> IResult<&str, Option<Token>> {
    opt(tag("/"))(input).map(|(remaining, parsed)| (remaining, parsed.map(|_| Token::Root)))
}

fn parse_dollar(input: &str) -> IResult<&str, Option<Token>> {
    let mut dollar = opt(tuple((tag("$"), alphanumeric1)));
    dollar(input).map(|(remaining, parsed)| {
        (
            remaining,
            parsed.map(|(_pr, name)| Token::Dollar(name.as_bytes().to_vec())),
        )
    })
}

fn parse_match_suffix(input: &str) -> IResult<&str, Option<MatchToken>> {
    let mut suffix = opt(alt((tag("#"), tag("+"), alphanumeric1)));
    suffix(input).map(|(remaining, parsed)| {
        (
            remaining,
            parsed.map(|name| match name {
                "#" => MatchToken::MultiLevel,
                "+" => MatchToken::SingleLevel,
                _ => MatchToken::Token(Token::Topic(name.as_bytes().to_vec())),
            }),
        )
    })
}

fn parse_topic_suffix(input: &str) -> IResult<&str, Option<Token>> {
    let mut suffix = opt(alphanumeric1);
    suffix(input).map(|(remaining, parsed)| {
        (
            remaining,
            parsed.map(|name| Token::Topic(name.as_bytes().to_vec())),
        )
    })
}

fn parse_tokens(input: &str) -> IResult<&str, Vec<Token>> {
    let token = alphanumeric0;
    let mut tokens = many0(terminated(token, tag("/")));
    tokens(input).map(|(remaining, parsed)| {
        (
            remaining,
            parsed
                .into_iter()
                .filter_map(|name| match name {
                    v if !v.is_empty() => Some(Token::Topic(name.as_bytes().to_vec())),
                    _ => None,
                })
                .collect(),
        )
    })
}

fn parse_match_tokens(input: &str) -> IResult<&str, Vec<MatchToken>> {
    let token = alt((tag("+"), alphanumeric0));
    let mut tokens = many0(terminated(token, tag("/")));
    tokens(input).map(|(remaining, parsed)| {
        (
            remaining,
            parsed
                .into_iter()
                .filter_map(|name| match name {
                    "+" => Some(MatchToken::SingleLevel),
                    v if !v.is_empty() => {
                        Some(MatchToken::Token(Token::Topic(name.as_bytes().to_vec())))
                    }
                    _ => None,
                })
                .collect(),
        )
    })
}

pub fn parse_topic_filter(topic_filter: &str) -> Result<Vec<MatchToken>> {
    let mut pattern = vec![];

    let err = |_| anyhow!("could not parse:  {}", topic_filter);

    let (topic_filter, root) = parse_root(topic_filter)
        .map_err(err)
        .context("parse root")?;
    let topic_filter = if let Some(root) = root {
        pattern.push(MatchToken::Token(root));
        topic_filter
    } else {
        let (topic_filter, prefix) = parse_dollar(topic_filter)
            .map_err(err)
            .context("parse dollar")?;
        if let Some(prefix) = prefix {
            pattern.push(MatchToken::Token(prefix));
        };
        topic_filter
    };

    let (topic_filter, prefix) = parse_match_tokens(topic_filter)
        .map_err(err)
        .context("parse token")?;
    pattern.extend(prefix);

    let (topic_filter, suffix) = parse_match_suffix(topic_filter)
        .map_err(err)
        .context("parse suffix")?;
    if let Some(suffix) = suffix {
        pattern.push(suffix)
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

pub fn parse_topic_name(topic_name: &str) -> Result<Vec<Token>> {
    let mut pattern = vec![];

    let err = |_| anyhow!("could not parse topic:  {}", topic_name);

    let (topic_name, root) = parse_root(topic_name).map_err(err).context("parse root")?;
    let topic_name = if let Some(root) = root {
        pattern.push(root);
        topic_name
    } else {
        let (topic_name, prefix) = parse_dollar(topic_name)
            .map_err(err)
            .context("parse topic dollar")?;
        if let Some(prefix) = prefix {
            pattern.push(prefix);
        };
        topic_name
    };

    let (topic_name, prefix) = parse_tokens(topic_name)
        .map_err(err)
        .context("parse token name")?;
    pattern.extend(prefix);

    let (topic_name, suffix) = parse_topic_suffix(topic_name)
        .map_err(err)
        .context("parse topic suffix")?;
    if let Some(suffix) = suffix {
        pattern.push(suffix)
    };

    if topic_name.is_empty() {
        Ok(pattern)
    } else {
        Err(anyhow!("topic name {:?} could not be parsed", topic_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_topic_name() {
        assert_eq!(
            parse_topic_name("test").unwrap(),
            vec![Token::Topic("test".as_bytes().to_vec())]
        );
        assert_eq!(
            parse_topic_name("topic/").unwrap(),
            vec![Token::Topic("topic".as_bytes().to_vec())]
        );
        assert_eq!(
            parse_topic_name("topic/Test/").unwrap(),
            [
                Token::Topic("topic".as_bytes().to_vec()),
                Token::Topic("Test".as_bytes().to_vec())
            ]
        );
        assert!(parse_topic_name("topic/+/").is_err(),);
        assert!(parse_topic_name("+/topic/").is_err(),);
        assert!(parse_topic_name("+/+/").is_err(),);

        assert!(parse_topic_name("topic/#").is_err(),);
        assert!(parse_topic_name("topic/$ff").is_err());

        assert!(parse_topic_name("topic/Test/#").is_err(),);
        assert!(parse_topic_name("topic/+/#").is_err(),);
        assert!(parse_topic_name("+/topic/#").is_err(),);
        assert!(parse_topic_name("+/+/#").is_err(),);

        assert_eq!(
            parse_topic_name("/topic/").unwrap(),
            [Token::Root, Token::Topic("topic".as_bytes().to_vec())]
        );
        assert_eq!(
            parse_topic_name("/topic/Test/").unwrap(),
            [
                Token::Root,
                Token::Topic("topic".as_bytes().to_vec()),
                Token::Topic("Test".as_bytes().to_vec())
            ]
        );
        assert!(parse_topic_name("/topic/+/").is_err());
        assert!(parse_topic_name("/+/topic/").is_err(),);
        assert!(parse_topic_name("/+/+/").is_err(),);

        assert!(parse_topic_name("/topic/#").is_err(),);

        assert!(parse_topic_name("/topic/#/topic").is_err());

        assert!(parse_topic_name("/topic/Test/#").is_err(),);
        assert!(parse_topic_name("/topic/+/#").is_err(),);
        assert!(parse_topic_name("/+/topic/#").is_err(),);
        assert!(parse_topic_name("/+/+/#").is_err(),);

        assert_eq!(
            parse_topic_name("$topic/").unwrap(),
            [Token::Dollar("topic".as_bytes().to_vec())]
        );
        assert_eq!(
            parse_topic_name("$topic/Test/").unwrap(),
            [
                Token::Dollar("topic".as_bytes().to_vec()),
                Token::Topic("Test".as_bytes().to_vec())
            ]
        );
        assert!(parse_topic_name("$topic/+/").is_err(),);
        assert!(parse_topic_name("$+/topic/").is_err());
        assert!(parse_topic_name("$+/+/").is_err());

        assert!(parse_topic_name("$topic/#").is_err(),);
        assert!(parse_topic_name("$topic/Test/#").is_err(),);
        assert!(parse_topic_name("$+/topic/#").is_err());
        assert!(parse_topic_name("$+/+/#").is_err());
    }

    #[test]
    fn test_parse_topic_filter() {
        assert_eq!(
            parse_topic_filter("+/").unwrap(),
            vec![MatchToken::SingleLevel]
        );
        assert_eq!(
            parse_topic_filter("/+").unwrap(),
            vec![MatchToken::Token(Token::Root), MatchToken::SingleLevel]
        );
        assert_eq!(
            parse_topic_filter("+").unwrap(),
            vec![MatchToken::SingleLevel]
        );
        assert_eq!(
            parse_topic_filter("topic/").unwrap(),
            vec![MatchToken::Token(Token::Topic("topic".as_bytes().to_vec()))]
        );
        assert_eq!(
            parse_topic_filter("topic/Test/").unwrap(),
            [
                MatchToken::Token(Token::Topic("topic".as_bytes().to_vec())),
                MatchToken::Token(Token::Topic("Test".as_bytes().to_vec()))
            ]
        );
        assert_eq!(
            parse_topic_filter("topic/+/").unwrap(),
            [
                MatchToken::Token(Token::Topic("topic".as_bytes().to_vec())),
                MatchToken::SingleLevel
            ]
        );
        assert_eq!(
            parse_topic_filter("+/topic/").unwrap(),
            [
                MatchToken::SingleLevel,
                MatchToken::Token(Token::Topic("topic".as_bytes().to_vec()))
            ]
        );
        assert_eq!(
            parse_topic_filter("+/+/").unwrap(),
            [MatchToken::SingleLevel, MatchToken::SingleLevel]
        );

        assert_eq!(
            parse_topic_filter("topic/#").unwrap(),
            [
                MatchToken::Token(Token::Topic("topic".as_bytes().to_vec())),
                MatchToken::MultiLevel
            ]
        );
        assert!(parse_topic_filter("topic/$ff").is_err());

        assert_eq!(
            parse_topic_filter("topic/Test/#").unwrap(),
            [
                MatchToken::Token(Token::Topic("topic".as_bytes().to_vec())),
                MatchToken::Token(Token::Topic("Test".as_bytes().to_vec())),
                MatchToken::MultiLevel
            ]
        );
        assert_eq!(
            parse_topic_filter("topic/+/#").unwrap(),
            [
                MatchToken::Token(Token::Topic("topic".as_bytes().to_vec())),
                MatchToken::SingleLevel,
                MatchToken::MultiLevel
            ]
        );
        assert_eq!(
            parse_topic_filter("+/topic/#").unwrap(),
            [
                MatchToken::SingleLevel,
                MatchToken::Token(Token::Topic("topic".as_bytes().to_vec())),
                MatchToken::MultiLevel
            ]
        );
        assert_eq!(
            parse_topic_filter("+/+/#").unwrap(),
            [
                MatchToken::SingleLevel,
                MatchToken::SingleLevel,
                MatchToken::MultiLevel
            ]
        );

        assert_eq!(
            parse_topic_filter("/topic/").unwrap(),
            [
                MatchToken::Token(Token::Root),
                MatchToken::Token(Token::Topic("topic".as_bytes().to_vec()))
            ]
        );
        assert_eq!(
            parse_topic_filter("/topic/Test/").unwrap(),
            [
                MatchToken::Token(Token::Root),
                MatchToken::Token(Token::Topic("topic".as_bytes().to_vec())),
                MatchToken::Token(Token::Topic("Test".as_bytes().to_vec()))
            ]
        );
        assert_eq!(
            parse_topic_filter("/topic/+/").unwrap(),
            [
                MatchToken::Token(Token::Root),
                MatchToken::Token(Token::Topic("topic".as_bytes().to_vec())),
                MatchToken::SingleLevel
            ]
        );
        assert_eq!(
            parse_topic_filter("/+/topic/").unwrap(),
            [
                MatchToken::Token(Token::Root),
                MatchToken::SingleLevel,
                MatchToken::Token(Token::Topic("topic".as_bytes().to_vec()))
            ]
        );
        assert_eq!(
            parse_topic_filter("/+/+/").unwrap(),
            [
                MatchToken::Token(Token::Root),
                MatchToken::SingleLevel,
                MatchToken::SingleLevel
            ]
        );

        assert_eq!(
            parse_topic_filter("/topic/#").unwrap(),
            [
                MatchToken::Token(Token::Root),
                MatchToken::Token(Token::Topic("topic".as_bytes().to_vec())),
                MatchToken::MultiLevel
            ]
        );

        assert!(parse_topic_filter("/topic/#/topic").is_err());

        assert_eq!(
            parse_topic_filter("/topic/Test/#").unwrap(),
            [
                MatchToken::Token(Token::Root),
                MatchToken::Token(Token::Topic("topic".as_bytes().to_vec())),
                MatchToken::Token(Token::Topic("Test".as_bytes().to_vec())),
                MatchToken::MultiLevel
            ]
        );
        assert_eq!(
            parse_topic_filter("/topic/+/#").unwrap(),
            [
                MatchToken::Token(Token::Root),
                MatchToken::Token(Token::Topic("topic".as_bytes().to_vec())),
                MatchToken::SingleLevel,
                MatchToken::MultiLevel
            ]
        );
        assert_eq!(
            parse_topic_filter("/+/topic/#").unwrap(),
            [
                MatchToken::Token(Token::Root),
                MatchToken::SingleLevel,
                MatchToken::Token(Token::Topic("topic".as_bytes().to_vec())),
                MatchToken::MultiLevel
            ]
        );
        assert_eq!(
            parse_topic_filter("/+/+/#").unwrap(),
            [
                MatchToken::Token(Token::Root),
                MatchToken::SingleLevel,
                MatchToken::SingleLevel,
                MatchToken::MultiLevel
            ]
        );

        assert_eq!(
            parse_topic_filter("$topic/").unwrap(),
            [MatchToken::Token(Token::Dollar(
                "topic".as_bytes().to_vec()
            ))]
        );
        assert_eq!(
            parse_topic_filter("$topic/Test/").unwrap(),
            [
                MatchToken::Token(Token::Dollar("topic".as_bytes().to_vec())),
                MatchToken::Token(Token::Topic("Test".as_bytes().to_vec()))
            ]
        );
        assert_eq!(
            parse_topic_filter("$topic/+/").unwrap(),
            [
                MatchToken::Token(Token::Dollar("topic".as_bytes().to_vec())),
                MatchToken::SingleLevel
            ]
        );
        assert!(parse_topic_filter("$+/topic/").is_err());
        assert!(parse_topic_filter("$+/+/").is_err());

        assert_eq!(
            parse_topic_filter("$topic/#").unwrap(),
            [
                MatchToken::Token(Token::Dollar("topic".as_bytes().to_vec())),
                MatchToken::MultiLevel
            ]
        );
        assert_eq!(
            parse_topic_filter("$topic/Test/#").unwrap(),
            [
                MatchToken::Token(Token::Dollar("topic".as_bytes().to_vec())),
                MatchToken::Token(Token::Topic("Test".as_bytes().to_vec())),
                MatchToken::MultiLevel
            ]
        );
        assert_eq!(
            parse_topic_filter("$topic/+/#").unwrap(),
            [
                MatchToken::Token(Token::Dollar("topic".as_bytes().to_vec())),
                MatchToken::SingleLevel,
                MatchToken::MultiLevel
            ]
        );
        assert!(parse_topic_filter("$+/topic/#").is_err());
        assert!(parse_topic_filter("$+/+/#").is_err());
    }
}
