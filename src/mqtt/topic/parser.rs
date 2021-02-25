use anyhow::{anyhow, Context, Result};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alphanumeric0, alphanumeric1};
use nom::combinator::opt;
use nom::multi::many0;
use nom::sequence::{terminated, tuple};
use nom::IResult;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum Token<'a> {
    Root,
    Topic(&'a str),
    SingleLevel,
    MultiLevel,
    Dollar(&'a str),
}

fn parse_root(input: &str) -> IResult<&str, Option<Token>> {
    opt(tag("/"))(input).map(|(remaining, parsed)| (remaining, parsed.map(|_| Token::Root)))
}

fn parse_dollar(input: &str) -> IResult<&str, Option<Token>> {
    let mut dollar = opt(tuple((tag("$"), alphanumeric1)));
    dollar(input).map(|(remaining, parsed)| {
        (
            remaining,
            parsed.map(|(pr, name)| match pr {
                "$" => Token::Dollar(name),
                _ => unreachable!(),
            }),
        )
    })
}

fn parse_suffix(input: &str) -> IResult<&str, Option<Token>> {
    let mut suffix = opt(alt((tag("#"), alphanumeric1)));
    suffix(input).map(|(remaining, parsed)| {
        (
            remaining,
            parsed.map(|name| match name {
                "#" => Token::MultiLevel,
                _ => Token::Topic(name),
            }),
        )
    })
}

fn parse_tokens(input: &str) -> IResult<&str, Vec<Token>> {
    let token = alt((tag("+"), alphanumeric0));
    let mut tokens = many0(terminated(token, tag("/")));
    tokens(input).map(|(remaining, parsed)| {
        (
            remaining,
            parsed
                .into_iter()
                .filter_map(|name| match name {
                    "+" => Some(Token::SingleLevel),
                    v if !v.is_empty() => Some(Token::Topic(name)),
                    _ => None,
                })
                .collect(),
        )
    })
}

pub(crate) fn parse(topic_filter: &str) -> Result<Vec<Token>> {
    let mut pattern = vec![];

    let err = |_| anyhow!("could not parse:  {}", topic_filter);

    let (topic_filter, root) = parse_root(topic_filter)
        .map_err(err)
        .context("parse root")?;
    let topic_filter = if let Some(root) = root {
        pattern.push(root);
        topic_filter
    } else {
        let (topic_filter, prefix) = parse_dollar(topic_filter)
            .map_err(err)
            .context("parse dollar")?;
        if let Some(prefix) = prefix {
            pattern.push(prefix);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default, Eq, PartialEq, Clone, Copy)]
    struct TestTopic<'a> {
        test: &'a str,
    }

    #[test]
    fn test_nom_parser() {
        assert_eq!(parse("topic/").unwrap(), vec![Token::Topic("topic")]);
        assert_eq!(
            parse("topic/Test/").unwrap(),
            [Token::Topic("topic"), Token::Topic("Test")]
        );
        assert_eq!(
            parse("topic/+/").unwrap(),
            [Token::Topic("topic"), Token::SingleLevel]
        );
        assert_eq!(
            parse("+/topic/").unwrap(),
            [Token::SingleLevel, Token::Topic("topic")]
        );
        assert_eq!(
            parse("+/+/").unwrap(),
            [Token::SingleLevel, Token::SingleLevel]
        );

        assert_eq!(
            parse("topic/#").unwrap(),
            [Token::Topic("topic"), Token::MultiLevel]
        );
        assert!(parse("topic/$ff").is_err());

        assert_eq!(
            parse("topic/Test/#").unwrap(),
            [
                Token::Topic("topic"),
                Token::Topic("Test"),
                Token::MultiLevel
            ]
        );
        assert_eq!(
            parse("topic/+/#").unwrap(),
            [Token::Topic("topic"), Token::SingleLevel, Token::MultiLevel]
        );
        assert_eq!(
            parse("+/topic/#").unwrap(),
            [Token::SingleLevel, Token::Topic("topic"), Token::MultiLevel]
        );
        assert_eq!(
            parse("+/+/#").unwrap(),
            [Token::SingleLevel, Token::SingleLevel, Token::MultiLevel]
        );

        assert_eq!(
            parse("/topic/").unwrap(),
            [Token::Root, Token::Topic("topic")]
        );
        assert_eq!(
            parse("/topic/Test/").unwrap(),
            [Token::Root, Token::Topic("topic"), Token::Topic("Test")]
        );
        assert_eq!(
            parse("/topic/+/").unwrap(),
            [Token::Root, Token::Topic("topic"), Token::SingleLevel]
        );
        assert_eq!(
            parse("/+/topic/").unwrap(),
            [Token::Root, Token::SingleLevel, Token::Topic("topic")]
        );
        assert_eq!(
            parse("/+/+/").unwrap(),
            [Token::Root, Token::SingleLevel, Token::SingleLevel]
        );

        assert_eq!(
            parse("/topic/#").unwrap(),
            [Token::Root, Token::Topic("topic"), Token::MultiLevel]
        );

        assert!(parse("/topic/#/topic").is_err());

        assert_eq!(
            parse("/topic/Test/#").unwrap(),
            [
                Token::Root,
                Token::Topic("topic"),
                Token::Topic("Test"),
                Token::MultiLevel
            ]
        );
        assert_eq!(
            parse("/topic/+/#").unwrap(),
            [
                Token::Root,
                Token::Topic("topic"),
                Token::SingleLevel,
                Token::MultiLevel
            ]
        );
        assert_eq!(
            parse("/+/topic/#").unwrap(),
            [
                Token::Root,
                Token::SingleLevel,
                Token::Topic("topic"),
                Token::MultiLevel
            ]
        );
        assert_eq!(
            parse("/+/+/#").unwrap(),
            [
                Token::Root,
                Token::SingleLevel,
                Token::SingleLevel,
                Token::MultiLevel
            ]
        );

        assert_eq!(parse("$topic/").unwrap(), [Token::Dollar("topic")]);
        assert_eq!(
            parse("$topic/Test/").unwrap(),
            [Token::Dollar("topic"), Token::Topic("Test")]
        );
        assert_eq!(
            parse("$topic/+/").unwrap(),
            [Token::Dollar("topic"), Token::SingleLevel]
        );
        assert!(parse("$+/topic/").is_err());
        assert!(parse("$+/+/").is_err());

        assert_eq!(
            parse("$topic/#").unwrap(),
            [Token::Dollar("topic"), Token::MultiLevel]
        );
        assert_eq!(
            parse("$topic/Test/#").unwrap(),
            [
                Token::Dollar("topic"),
                Token::Topic("Test"),
                Token::MultiLevel
            ]
        );
        assert_eq!(
            parse("$topic/+/#").unwrap(),
            [
                Token::Dollar("topic"),
                Token::SingleLevel,
                Token::MultiLevel
            ]
        );
        assert!(parse("$+/topic/#").is_err());
        assert!(parse("$+/+/#").is_err());
    }
}
