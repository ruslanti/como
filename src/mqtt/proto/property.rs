use std::convert::{TryFrom, TryInto};
use std::mem::size_of_val;

use anyhow::{anyhow, Result};
use bytes::Bytes;

use crate::mqtt::proto::types::MqttString;
use crate::mqtt::proto::types::QoS;

macro_rules! check_and_set {
    ($self:ident, $property:ident, $value: expr) => {
        match $self.$property.replace($value) {
            None    => Ok($self),
            Some(_) => Err(anyhow!("protocol error"))
        }
    };
}

macro_rules! check_size_of {
    ($self:ident, $property:ident) => {
        match &$self.$property {
            None    => 0,
            Some(v) => size_of_val(v) + 1
        }
    };
}

macro_rules! check_size_of_string {
    ($self:ident, $property:ident) => {
        match &$self.$property {
            None    => 0,
            Some(v) => v.len() + 3
        }
    };
}

macro_rules! encode_property_u8 {
    ($writer:ident, $property:ident, $value:expr) => {
        if let Some(value) = $value {
            end_of_stream!($writer.capacity() < 1, "$value id");
            $writer.put_u8(Property::$property as u8);
            end_of_stream!($writer.capacity() < 1, "$value");
            $writer.put_u8(value);
        }
    }
}

macro_rules! encode_property_u16 {
    ($writer:ident, $property:ident, $value:expr) => {
        if let Some(value) = $value {
            end_of_stream!($writer.capacity() < 1, "$value id");
            $writer.put_u8(Property::$property as u8);
            end_of_stream!($writer.capacity() < 2, "$value");
            $writer.put_u16(value);
        }
    }
}

macro_rules! encode_property_u32 {
    ($writer:ident, $property:ident, $value:expr) => {
        if let Some(value) = $value {
            end_of_stream!($writer.capacity() < 1, "$value id");
            $writer.put_u8(Property::$property as u8);
            end_of_stream!($writer.capacity() < 4, "$value");
            $writer.put_u32(value);
        }
    }
}

macro_rules! encode_property_string {
    ($writer:ident, $property:ident, $value:expr) => {
        if let Some(value) = $value {
            end_of_stream!($writer.capacity() < 1, "$value id");
            $writer.put_u8(Property::$property as u8);
            end_of_stream!($writer.capacity() < value.len(), "$value");
            encode_utf8_string($writer, value)?;
        }
    }
}

macro_rules! encode_property_user_properties {
    ($writer:ident, $property:ident, $value:expr) => {
        for (first, second) in $value {
            end_of_stream!($writer.capacity() < 1, "$value");
            $writer.put_u8(Property::$property as u8);
            encode_utf8_string($writer, first)?;
            encode_utf8_string($writer, second)?;
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Property {
    PayloadFormatIndicator          = 0x01,
    MessageExpireInterval           = 0x02,
    ContentType                     = 0x03,
    ResponseTopic                   = 0x08,
    CorrelationData                 = 0x09,
    SubscriptionIdentifier          = 0x0B,
    SessionExpireInterval           = 0x11,
    AssignedClientIdentifier        = 0x12,
    ServerKeepAlive                 = 0x13,
    AuthenticationMethod            = 0x15,
    AuthenticationData              = 0x16,
    RequestProblemInformation       = 0x17,
    WillDelayInterval               = 0x18,
    RequestResponseInformation      = 0x19,
    ResponseInformation             = 0x1A,
    ServerReference                 = 0x1C,
    ReasonString                    = 0x1F,
    ReceiveMaximum                  = 0x21,
    TopicAliasMaximum               = 0x22,
    TopicAlias                      = 0x23,
    MaximumQoS                      = 0x24,
    RetainAvailable                 = 0x25,
    UserProperty                    = 0x26,
    MaximumPacketSize               = 0x27,
    WildcardSubscriptionAvailable   = 0x28,
    SubscriptionIdentifierAvailable = 0x29,
    SharedSubscriptionAvailable     = 0x2A
}

impl TryFrom<u32> for Property {
    type Error = anyhow::Error;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Property::PayloadFormatIndicator),
            0x02 => Ok(Property::MessageExpireInterval),
            0x03 => Ok(Property::ContentType),
            0x08 => Ok(Property::ResponseTopic),
            0x09 => Ok(Property::CorrelationData),
            0x0B => Ok(Property::SubscriptionIdentifier),
            0x11 => Ok(Property::SessionExpireInterval),
            0x12 => Ok(Property::AssignedClientIdentifier),
            0x13 => Ok(Property::ServerKeepAlive),
            0x15 => Ok(Property::AuthenticationMethod),
            0x16 => Ok(Property::AuthenticationData),
            0x17 => Ok(Property::RequestProblemInformation),
            0x18 => Ok(Property::WillDelayInterval),
            0x19 => Ok(Property::RequestResponseInformation),
            0x1A => Ok(Property::ResponseInformation),
            0x1C => Ok(Property::ServerReference),
            0x1F => Ok(Property::ReasonString),
            0x21 => Ok(Property::ReceiveMaximum),
            0x22 => Ok(Property::TopicAliasMaximum),
            0x23 => Ok(Property::TopicAlias),
            0x24 => Ok(Property::MaximumQoS),
            0x25 => Ok(Property::RetainAvailable),
            0x26 => Ok(Property::UserProperty),
            0x27 => Ok(Property::MaximumPacketSize),
            0x28 => Ok(Property::WildcardSubscriptionAvailable),
            0x29 => Ok(Property::SubscriptionIdentifierAvailable),
            0x2A => Ok(Property::SharedSubscriptionAvailable),
            _ => Err(anyhow!("invalid property: {:x}", value)),
        }
    }
}

pub trait PropertiesLength {
    fn len(&self) -> usize;
}

#[derive(Debug, Eq, PartialEq)]
pub struct WillProperties {
    pub will_delay_interval: u32,
    pub payload_format_indicator: bool,
    pub message_expire_interval: Option<u32>,
    pub content_type: Option<MqttString>,
    pub response_topic: Option<MqttString>,
    pub correlation_data: Option<Bytes>,
    pub user_properties: Vec<(MqttString, MqttString)>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ConnectProperties {
    pub session_expire_interval: u32,
    pub receive_maximum: u16,
    pub maximum_packet_size: u32,
    pub topic_alias_maximum: u16,
    pub request_response_information: bool,
    pub request_problem_information: bool,
    pub user_properties: Vec<(MqttString, MqttString)>,
    pub authentication_method: Option<MqttString>
}

#[derive(Debug, Eq, PartialEq)]
pub struct ConnAckProperties {
    pub session_expire_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_qos: Option<QoS>,
    pub retain_available: Option<bool>,
    pub maximum_packet_size: Option<u32>,
    pub assigned_client_identifier: Option<MqttString>,
    pub topic_alias_maximum: Option<u16>,
    pub reason_string: Option<MqttString>,
    pub user_properties: Vec<(MqttString, MqttString)>,
    pub wildcard_subscription_available: Option<bool>,
    pub subscription_identifier_available: Option<bool>,
    pub shared_subscription_available: Option<bool>,
    pub server_keep_alive: Option<u16>,
    pub response_information: Option<MqttString>,
    pub server_reference: Option<MqttString>,
    pub authentication_method: Option<MqttString>,
    pub authentication_data: Option<Bytes>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PublishProperties {
    pub payload_format_indicator: Option<bool>,
    pub message_expire_interval: Option<u32>,
    pub topic_alias: Option<u16>,
    pub response_topic: Option<MqttString>,
    pub correlation_data: Option<Bytes>,
    pub user_properties: Vec<(MqttString, MqttString)>,
    pub subscription_identifier: Option<u32>,
    pub content_type: Option<MqttString>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PubResProperties {
    pub reason_string: Option<MqttString>,
    pub user_properties: Vec<(MqttString, MqttString)>,
}


#[derive(Debug, Eq, PartialEq)]
pub struct DisconnectProperties {
    pub session_expire_interval: Option<u32>,
    pub reason_string: Option<MqttString>,
    pub user_properties: Vec<(MqttString, MqttString)>,
    pub server_reference: Option<MqttString>
}

#[derive(Debug, Eq, PartialEq)]
pub struct SubscribeProperties {
    pub subscription_identifier: Option<u32>,
    pub user_properties: Vec<(MqttString, MqttString)>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct SubAckProperties {
    pub reason_string: Option<MqttString>,
    pub user_properties: Vec<(MqttString, MqttString)>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct UnSubscribeProperties {
    pub user_properties: Vec<(MqttString, MqttString)>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct AuthProperties {
    pub authentication_method: Option<MqttString>,
    pub authentication_data: Option<Bytes>,
    pub reason_string: Option<MqttString>,
    pub user_properties: Vec<(MqttString, MqttString)>,
}

impl PropertiesLength for ConnAckProperties {
    fn len(&self) -> usize {
        let mut len = check_size_of!(self, session_expire_interval);
        len += check_size_of!(self, receive_maximum);
        len += check_size_of!(self, maximum_qos);
        len += check_size_of!(self, retain_available);
        len += check_size_of!(self, maximum_packet_size);
        len += check_size_of_string!(self, assigned_client_identifier);
        len += check_size_of!(self, topic_alias_maximum) ;
        len += check_size_of_string!(self, reason_string);
        len += self.user_properties.iter().map(|(x, y)| 5 + x.len() + y.len()).sum::<usize>();
        len
    }
}

impl PropertiesLength for PubResProperties {
    fn len(&self) -> usize {
        let mut len = check_size_of_string!(self, reason_string);
        len += self.user_properties.iter().map(|(x, y)| 5 + x.len() + y.len()).sum::<usize>();
        len
    }
}

impl PropertiesLength for DisconnectProperties {
    fn len(&self) -> usize {
        let mut len = check_size_of!(self, session_expire_interval);
        len += check_size_of_string!(self, reason_string);
        len += self.user_properties.iter().map(|(x, y)| 5 + x.len() + y.len()).sum::<usize>();
        len += check_size_of_string!(self, server_reference);
        len
    }
}

impl PropertiesLength for SubAckProperties {
    fn len(&self) -> usize {
        let mut len = check_size_of_string!(self, reason_string);
        len += self.user_properties.iter().map(|(x, y)| 5 + x.len() + y.len()).sum::<usize>();
        len
    }
}

pub struct PropertiesBuilder {
    payload_format_indicator: Option<bool>,
    message_expire_interval: Option<u32>,
    content_type: Option<MqttString>,
    response_topic: Option<MqttString>,
    correlation_data: Option<Bytes>,
    subscription_identifier: Option<u32>,
    session_expire_interval: Option<u32>,
    assigned_client_identifier: Option<MqttString>,
    server_keep_alive: Option<u16>,
    authentication_method: Option<MqttString>,
    authentication_data: Option<Bytes>,
    request_problem_information: Option<bool>,
    will_delay_interval: Option<u32>,
    request_response_information: Option<bool>,
    response_information: Option<MqttString>,
    server_reference: Option<MqttString>,
    reason_string: Option<MqttString>,
    receive_maximum: Option<u16>,
    topic_alias_maximum: Option<u16>,
    topic_alias: Option<u16>,
    maximum_qos: Option<QoS>,
    retain_available: Option<bool>,
    user_properties: Vec<(MqttString, MqttString)>,
    maximum_packet_size: Option<u32>,
    wildcard_subscription_available: Option<bool>,
    subscription_identifier_available: Option<bool>,
    shared_subscription_available: Option<bool>
}

impl PropertiesBuilder {
    pub fn new() -> Self {
        return PropertiesBuilder{
            session_expire_interval: None,
            assigned_client_identifier: None,
            receive_maximum: None,
            maximum_packet_size: None,
            wildcard_subscription_available: None,
            subscription_identifier_available: None,
            topic_alias_maximum: None,
            topic_alias: None,
            maximum_qos: None,
            request_response_information: None,
            response_information: None,
            server_reference: None,
            request_problem_information: None,
            user_properties: vec![],
            authentication_method: None,
            will_delay_interval: None,
            payload_format_indicator: None,
            message_expire_interval: None,
            content_type: None,
            response_topic: None,
            correlation_data: None,
            subscription_identifier: None,
            server_keep_alive: None,
            authentication_data: None,
            reason_string: None,
            retain_available: None,
            shared_subscription_available: None
        };
    }
    
    pub fn session_expire_interval(mut self, value: u32) -> Result<Self> {
        check_and_set!(self, session_expire_interval, value)
    }
    pub fn receive_maximum(mut self, value: u16) -> Result<Self> {
        check_and_set!(self, receive_maximum, value)
    }
    pub fn maximum_packet_size(mut self, value: u32) -> Result<Self> {
        check_and_set!(self, maximum_packet_size, value)
    }
    pub fn topic_alias_maximum(mut self, value: u16) -> Result<Self> {
        check_and_set!(self, topic_alias_maximum, value)
    }
    pub fn request_response_information(mut self, value: u8) -> Result<Self> {
        check_and_set!(self, request_response_information, value != 0)
    }
    pub fn request_problem_information(mut self, value: u8) -> Result<Self> {
        check_and_set!(self, request_problem_information, value != 0)
    }
    pub fn user_properties(mut self, value: (MqttString, MqttString)) -> Self {
        self.user_properties.push(value);
        self
    }
    pub fn authentication_method(mut self, value: MqttString) -> Result<Self> {
        check_and_set!(self, authentication_method, value)
    }
    pub fn will_delay_interval(mut self, value: u32) -> Result<Self> {
        check_and_set!(self, will_delay_interval, value)
    }
    pub fn payload_format_indicator(mut self, value: u8) -> Result<Self> {
        check_and_set!(self, payload_format_indicator, value != 0)
    }
    pub fn message_expire_interval(mut self, value: u32) -> Result<Self> {
        check_and_set!(self, message_expire_interval, value)
    }
    pub fn content_type(mut self, value: Option<MqttString>) -> Result<Self> {
        if let Some(v) = value {
            check_and_set!(self, content_type, v)
        } else { Err(anyhow!("empty content type")) }
    }
    pub fn response_topic(mut self, value: Option<MqttString>) -> Result<Self> {
        if let Some(v) = value {
            check_and_set!(self, response_topic, v)
        } else {
            Err(anyhow!("empty response topic"))
        }
    }
    pub fn server_reference(mut self, value: Option<MqttString>) -> Result<Self> {
        if let Some(v) = value {
            check_and_set!(self, server_reference, v)
        } else {
            Err(anyhow!("empty server reference"))
        }
    }
    pub fn correlation_data(mut self, value: Bytes) -> Result<Self> {
        check_and_set!(self, correlation_data, value)
    }
    pub fn maximum_qos(mut self, value: u8) -> Result<Self> {
        check_and_set!(self, maximum_qos, value.try_into()?)
    }
    pub fn retain_available(mut self, value: u8) -> Result<Self> {
        check_and_set!(self, retain_available, value != 0)
    }
    pub fn wildcard_subscription_available(mut self, value: u8) -> Result<Self> {
        check_and_set!(self, wildcard_subscription_available, value != 0)
    }
    pub fn subscription_identifier_available(mut self, value: u8) -> Result<Self> {
        check_and_set!(self, subscription_identifier_available, value != 0)
    }
    pub fn shared_subscription_available(mut self, value: u8) -> Result<Self> {
        check_and_set!(self, shared_subscription_available, value != 0)
    }
    pub fn server_keep_alive(mut self, value: u16) -> Result<Self> {
        check_and_set!(self, server_keep_alive, value)
    }
    pub fn topic_alias(mut self, value: u16) -> Result<Self> {
        check_and_set!(self, topic_alias, value)
    }
    pub fn subscription_identifier(mut self, value: u32) -> Result<Self> {
        check_and_set!(self, subscription_identifier, value)
    }

    pub fn will(self) -> WillProperties {
        WillProperties {
            will_delay_interval: self.will_delay_interval.unwrap_or(0),
            payload_format_indicator: self.payload_format_indicator.unwrap_or(false),
            message_expire_interval: self.message_expire_interval,
            content_type: self.content_type,
            response_topic: self.response_topic,
            correlation_data: self.correlation_data,
            user_properties: self.user_properties
        }
    }

    pub fn connect(self) -> ConnectProperties {
        ConnectProperties {
            session_expire_interval: self.session_expire_interval.unwrap_or(0),
            receive_maximum: self.receive_maximum.unwrap_or(u16::max_value()),
            maximum_packet_size: self.maximum_packet_size.unwrap_or(u32::max_value()),
            topic_alias_maximum: self.topic_alias_maximum.unwrap_or(0),
            request_response_information: self.request_response_information.unwrap_or(false),
            request_problem_information: self.request_problem_information.unwrap_or(true),
            user_properties: self.user_properties,
            authentication_method: self.authentication_method
        }
    }

    pub fn connack(self) -> ConnAckProperties {
        ConnAckProperties{
            session_expire_interval: self.session_expire_interval,
            receive_maximum: self.receive_maximum,
            maximum_qos: self.maximum_qos,
            retain_available: self.retain_available,
            maximum_packet_size: self.maximum_packet_size,
            assigned_client_identifier: self.assigned_client_identifier,
            topic_alias_maximum: self.topic_alias_maximum,
            reason_string: self.reason_string,
            user_properties: self.user_properties,
            wildcard_subscription_available: self.wildcard_subscription_available,
            subscription_identifier_available: self.subscription_identifier_available,
            shared_subscription_available: self.shared_subscription_available,
            server_keep_alive: self.server_keep_alive,
            response_information: self.response_information,
            server_reference: self.server_reference,
            authentication_method: self.authentication_method,
            authentication_data: self.authentication_data
        }
    }

    pub fn publish(self) -> PublishProperties {
        PublishProperties {
            payload_format_indicator: self.payload_format_indicator,
            message_expire_interval: self.message_expire_interval,
            topic_alias: self.topic_alias,
            response_topic: self.response_topic,
            correlation_data: self.correlation_data,
            user_properties: self.user_properties,
            subscription_identifier: self.subscription_identifier,
            content_type: self.content_type
        }
    }

    pub fn pubres(self) -> PubResProperties {
        PubResProperties{
            reason_string: self.reason_string,
            user_properties: self.user_properties
        }
    }

    pub fn disconnect(self) -> DisconnectProperties {
        DisconnectProperties {
            session_expire_interval: self.session_expire_interval,
            reason_string: self.reason_string,
            user_properties: self.user_properties,
            server_reference: self.server_reference
        }
    }

    pub fn subscribe(self) -> SubscribeProperties {
        SubscribeProperties {
            subscription_identifier: self.subscription_identifier,
            user_properties: self.user_properties
        }
    }

    pub fn unsubscribe(self) -> UnSubscribeProperties {
        UnSubscribeProperties {
            user_properties: self.user_properties
        }
    }

    pub fn auth(self) -> AuthProperties {
        AuthProperties {
            authentication_method: self.authentication_method,
            authentication_data: self.authentication_data,
            reason_string: self.reason_string,
            user_properties: self.user_properties
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_properties_default() {
        assert_eq!( PropertiesBuilder::new().connect(),
            ConnectProperties{
            session_expire_interval: 0,
            receive_maximum: 65535,
            maximum_packet_size: 4294967295,
            topic_alias_maximum: 0,
            request_response_information: false,
            request_problem_information: true,
            user_properties: vec![],
            authentication_method: None}
        );
    }
    #[test]
    fn test_connection_properties_fill() {
        let mut builder = PropertiesBuilder::new();
        builder = builder.session_expire_interval(20).unwrap();
        builder = builder.receive_maximum(1000).unwrap();
        builder = builder.maximum_packet_size(1024).unwrap();
        builder = builder.topic_alias_maximum(1024).unwrap();
        builder = builder.request_response_information(1).unwrap();
        builder = builder.request_problem_information(1).unwrap();
        builder = builder.user_properties((Bytes::from("username"), Bytes::from("admin")));
        builder = builder.user_properties((Bytes::from("password"), Bytes::from("12345")));
        assert_eq!( builder.connect(),
                    ConnectProperties{
                        session_expire_interval: 20,
                        receive_maximum: 1000,
                        maximum_packet_size: 1024,
                        topic_alias_maximum: 1024,
                        request_response_information: true,
                        request_problem_information: true,
                        user_properties: vec![(Bytes::from("username"), Bytes::from("admin")), (Bytes::from("password"), Bytes::from("12345"))],
                        authentication_method: None}
        );
    }

    #[test]
    fn test_properties_sei_dup() {
        let mut builder = PropertiesBuilder::new();
        builder = builder.session_expire_interval(20).unwrap();
        match builder.session_expire_interval(60) {
            Err(_) => assert!(true),
            _ => assert!(false, "should panic")
        }
    }
    #[test]
    fn test_properties_rm_dup() {
        let mut builder = PropertiesBuilder::new();
        builder = builder.receive_maximum(20).unwrap();
        match builder.receive_maximum(60) {
            Err(_) => assert!(true),
            _ => assert!(false, "should panic")
        }
    }

    #[test]
    fn test_will_properties_default() {
        assert_eq!( PropertiesBuilder::new().will(),
                    WillProperties {
                        will_delay_interval: 0,
                        payload_format_indicator: false,
                        message_expire_interval: None,
                        content_type: None,
                        response_topic: None,
                        correlation_data: None,
                        user_properties: vec![]
                    } 
        );
    }
    #[test]
    fn test_connack_properties_default_len() {
        let mut builder = PropertiesBuilder::new();
        builder = builder.session_expire_interval(20).unwrap();
        builder = builder.user_properties((Bytes::from("username"), Bytes::from("admin")));
        builder = builder.user_properties((Bytes::from("password"), Bytes::from("123456")));
        assert_eq!(42, builder.connack().len());
    }
}
