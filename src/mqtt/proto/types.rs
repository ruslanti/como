use core::fmt;
use std::convert::{TryFrom, TryInto};

use anyhow::anyhow;
use bytes::Bytes;
use serde::{de, Deserialize, Deserializer};
use serde::de::Visitor;

use crate::mqtt::proto::property::{
    AuthProperties, ConnAckProperties, ConnectProperties, DisconnectProperties, PublishProperties,
    PubResProperties, SubAckProperties, SubscribeProperties, UnSubscribeProperties,
    WillProperties,
};

pub type MqttString = Bytes;

#[macro_use]
macro_rules! end_of_stream {
    ($condition: expr, $context: expr) => {
        ensure!(!$condition, anyhow!("end of stream").context($context));
    };
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Hash)]
pub enum QoS {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

impl TryFrom<u8> for QoS {
    type Error = anyhow::Error;

    fn try_from(v: u8) -> anyhow::Result<Self> {
        match v {
            0 => Ok(QoS::AtMostOnce),
            1 => Ok(QoS::AtLeastOnce),
            2 => Ok(QoS::ExactlyOnce),
            _ => Err(anyhow!("malformed QoS: {}", v)),
        }
    }
}

impl Into<u8> for QoS {
    fn into(self) -> u8 {
        match self {
            QoS::AtMostOnce => 0,
            QoS::AtLeastOnce => 1,
            QoS::ExactlyOnce => 2,
        }
    }
}

struct QoSVisitor;

impl<'de> Visitor<'de> for QoSVisitor {
    type Value = QoS;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an integer between 0 and 2")
    }

    fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if let Ok(ret) = value.try_into() {
            Ok(ret)
        } else {
            Err(E::custom(format!("QoS out of range: {}", value)))
        }
    }
}

impl<'de> Deserialize<'de> for QoS {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_u8(QoSVisitor)
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash)]
pub enum Retain {
    SendAtTime,
    SendAtSubscribe,
    DoNotSend,
}

impl TryFrom<u8> for Retain {
    type Error = anyhow::Error;

    fn try_from(v: u8) -> anyhow::Result<Self> {
        match v {
            0 => Ok(Retain::SendAtTime),
            1 => Ok(Retain::SendAtSubscribe),
            2 => Ok(Retain::DoNotSend),
            _ => Err(anyhow!("malformed Retain: {}", v)),
        }
    }
}

impl Into<u8> for Retain {
    fn into(self) -> u8 {
        match self {
            Retain::SendAtTime => 0,
            Retain::SendAtSubscribe => 1,
            Retain::DoNotSend => 2,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum PacketType {
    CONNECT,
    CONNACK,
    PUBLISH { dup: bool, qos: QoS, retain: bool },
    PUBACK,
    PUBREC,
    PUBREL,
    PUBCOMP,
    SUBSCRIBE,
    SUBACK,
    UNSUBSCRIBE,
    UNSUBACK,
    PINGREQ,
    PINGRESP,
    DISCONNECT,
    AUTH,
}

impl TryFrom<u8> for PacketType {
    type Error = anyhow::Error;

    fn try_from(v: u8) -> anyhow::Result<Self> {
        match v {
            0x10 => Ok(PacketType::CONNECT),
            0x20 => Ok(PacketType::CONNACK),
            0x30..=0x3F => Ok(PacketType::PUBLISH {
                dup: v & 0b0000_1000 != 0,
                qos: ((v & 0b0000_0110) >> 1).try_into()?,
                retain: v & 0b0000_0001 != 0,
            }),
            0x40 => Ok(PacketType::PUBACK),
            0x50 => Ok(PacketType::PUBREC),
            0x62 => Ok(PacketType::PUBREL),
            0x70 => Ok(PacketType::PUBCOMP),
            0x82 => Ok(PacketType::SUBSCRIBE),
            0x90 => Ok(PacketType::SUBACK),
            0xA2 => Ok(PacketType::UNSUBSCRIBE),
            0xB0 => Ok(PacketType::UNSUBACK),
            0xC0 => Ok(PacketType::PINGREQ),
            0xD0 => Ok(PacketType::PINGRESP),
            0xE0 => Ok(PacketType::DISCONNECT),
            0xF0 => Ok(PacketType::AUTH),
            _ => Err(anyhow!("malformed control packet type: {}", v)),
        }
    }
}

impl Into<u8> for PacketType {
    fn into(self) -> u8 {
        match self {
            PacketType::CONNECT => 0x10,
            PacketType::CONNACK => 0x20,
            PacketType::PUBLISH { dup, qos, retain } => {
                0x30 | ((dup as u8) << 3) | ((qos as u8) << 1) | (retain as u8)
            }
            PacketType::PUBACK => 0x40,
            PacketType::PUBREC => 0x50,
            PacketType::PUBREL => 0x62,
            PacketType::PUBCOMP => 0x70,
            PacketType::SUBSCRIBE => 0x82,
            PacketType::SUBACK => 0x90,
            PacketType::UNSUBSCRIBE => 0xA2,
            PacketType::UNSUBACK => 0xB0,
            PacketType::PINGREQ => 0xC0,
            PacketType::PINGRESP => 0xD0,
            PacketType::DISCONNECT => 0xE0,
            PacketType::AUTH => 0xF0,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ReasonCode {
    Success = 0x00,
    DisconnectWithWill = 0x04,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationSpecificError = 0x83,
    UnsupportedProtocolVersion = 0x84,
    ClientIdentifiersNotValid = 0x85,
    BadUserNameOrPassword = 0x86,
    NotAuthorized = 0x87,
    ServerUnavailable = 0x88,
    ServerBusy = 0x89,
    Banned = 0x8A,
    ServerShuttingDown = 0x8B,
    BadAuthenticationMethod = 0x8C,
    KeepAliveTimeout = 0x8D,
    TopicFilterInvalid = 0x8F,
    TopicNameInvalid = 0x90,
    PacketIdentifierInUse = 0x91,
    PacketIdentifierNotFound = 0x92,
    PacketTooLarge = 0x95,
    QuotaExceeded = 0x97,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9A,
    QoSNotSupported = 0x9B,
    UseAnotherServer = 0x9C,
    ServerMoved = 0x9D,
    ConnectionRateExceeded = 0x9F,
}

impl TryFrom<u8> for ReasonCode {
    type Error = anyhow::Error;

    fn try_from(b: u8) -> Result<Self, Self::Error> {
        match b {
            0x00 => Ok(ReasonCode::Success),
            0x04 => Ok(ReasonCode::DisconnectWithWill),
            0x80 => Ok(ReasonCode::UnspecifiedError),
            0x81 => Ok(ReasonCode::MalformedPacket),
            0x82 => Ok(ReasonCode::ProtocolError),
            0x83 => Ok(ReasonCode::ImplementationSpecificError),
            0x84 => Ok(ReasonCode::UnsupportedProtocolVersion),
            0x85 => Ok(ReasonCode::ClientIdentifiersNotValid),
            0x86 => Ok(ReasonCode::BadUserNameOrPassword),
            0x87 => Ok(ReasonCode::NotAuthorized),
            0x88 => Ok(ReasonCode::ServerUnavailable),
            0x89 => Ok(ReasonCode::ServerBusy),
            0x8A => Ok(ReasonCode::Banned),
            0x8B => Ok(ReasonCode::ServerShuttingDown),
            0x8C => Ok(ReasonCode::BadAuthenticationMethod),
            0x8D => Ok(ReasonCode::KeepAliveTimeout),
            0x8F => Ok(ReasonCode::TopicFilterInvalid),
            0x90 => Ok(ReasonCode::TopicNameInvalid),
            0x91 => Ok(ReasonCode::PacketIdentifierInUse),
            0x92 => Ok(ReasonCode::PacketIdentifierNotFound),
            0x95 => Ok(ReasonCode::PacketTooLarge),
            0x97 => Ok(ReasonCode::QuotaExceeded),
            0x99 => Ok(ReasonCode::PayloadFormatInvalid),
            0x9A => Ok(ReasonCode::RetainNotSupported),
            0x9B => Ok(ReasonCode::QoSNotSupported),
            0x9C => Ok(ReasonCode::UseAnotherServer),
            0x9D => Ok(ReasonCode::ServerMoved),
            0x9F => Ok(ReasonCode::ConnectionRateExceeded),
            _ => Err(anyhow!("malformed control packet reason code: {}", b)),
        }
    }
}

impl Into<u8> for ReasonCode {
    fn into(self) -> u8 {
        match self {
            ReasonCode::Success => 0x00,
            ReasonCode::DisconnectWithWill => 0x04,
            ReasonCode::UnspecifiedError => 0x80,
            ReasonCode::MalformedPacket => 0x81,
            ReasonCode::ProtocolError => 0x82,
            ReasonCode::ImplementationSpecificError => 0x83,
            ReasonCode::UnsupportedProtocolVersion => 0x84,
            ReasonCode::ClientIdentifiersNotValid => 0x85,
            ReasonCode::BadUserNameOrPassword => 0x86,
            ReasonCode::NotAuthorized => 0x87,
            ReasonCode::ServerUnavailable => 0x88,
            ReasonCode::ServerBusy => 0x89,
            ReasonCode::Banned => 0x8A,
            ReasonCode::ServerShuttingDown => 0x8B,
            ReasonCode::BadAuthenticationMethod => 0x8C,
            ReasonCode::KeepAliveTimeout => 0x8D,
            ReasonCode::TopicFilterInvalid => 0x8F,
            ReasonCode::TopicNameInvalid => 0x90,
            ReasonCode::PacketIdentifierInUse => 0x91,
            ReasonCode::PacketIdentifierNotFound => 0x92,
            ReasonCode::PacketTooLarge => 0x95,
            ReasonCode::QuotaExceeded => 0x97,
            ReasonCode::PayloadFormatInvalid => 0x99,
            ReasonCode::RetainNotSupported => 0x9A,
            ReasonCode::QoSNotSupported => 0x9B,
            ReasonCode::UseAnotherServer => 0x9C,
            ReasonCode::ServerMoved => 0x9D,
            ReasonCode::ConnectionRateExceeded => 0x9F,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Will {
    pub qos: QoS,
    pub retain: bool,
    pub properties: WillProperties,
    pub topic: Option<MqttString>,
    pub payload: Bytes,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Connect {
    pub clean_start_flag: bool,
    pub keep_alive: u16,
    pub properties: ConnectProperties,
    pub client_identifier: Option<MqttString>,
    pub username: Option<MqttString>,
    pub password: Option<MqttString>,
    pub will: Option<Will>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ConnAck {
    pub session_present: bool,
    pub reason_code: ReasonCode,
    pub properties: ConnAckProperties,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Publish {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic_name: MqttString,
    pub packet_identifier: Option<u16>,
    pub properties: PublishProperties,
    pub payload: MqttString,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PubResp {
    pub packet_type: PacketType,
    pub packet_identifier: u16,
    pub reason_code: ReasonCode,
    pub properties: PubResProperties,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Disconnect {
    pub reason_code: ReasonCode,
    pub properties: DisconnectProperties,
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct SubOption {
    pub qos: QoS,
    pub nl: bool,
    pub rap: bool,
    pub retain: Retain,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Subscribe {
    pub packet_identifier: u16,
    pub properties: SubscribeProperties,
    pub topic_filters: Vec<(MqttString, SubOption)>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct SubAck {
    pub packet_identifier: u16,
    pub properties: SubAckProperties,
    pub reason_codes: Vec<ReasonCode>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct UnSubscribe {
    pub packet_identifier: u16,
    pub properties: UnSubscribeProperties,
    pub topic_filters: Vec<MqttString>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Auth {
    pub reason_code: ReasonCode,
    pub properties: AuthProperties,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ControlPacket {
    Connect(Connect),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck(PubResp),
    PubRec(PubResp),
    PubRel(PubResp),
    PubComp(PubResp),
    Subscribe(Subscribe),
    SubAck(SubAck),
    UnSubscribe(UnSubscribe),
    UnSubAck(SubAck),
    PingReq,
    PingResp,
    Disconnect(Disconnect),
    Auth(Auth),
}

pub enum PacketPart {
    FixedHeader,
    VariableHeader {
        remaining: usize,
        packet_type: PacketType,
    },
}

#[derive(Debug)]
pub struct MQTTCodec {
    pub part: PacketPart,
}

impl fmt::Debug for PacketPart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PacketPart::FixedHeader => write!(f, "PacketPart::FixedHeader"),
            PacketPart::VariableHeader {
                remaining,
                packet_type,
            } => f
                .debug_struct("PacketPart::VariableHeader")
                .field("remaining", &remaining)
                .field("packet_type", &packet_type)
                .finish(),
        }
    }
}

impl MQTTCodec {
    pub fn new() -> Self {
        MQTTCodec {
            part: PacketPart::FixedHeader,
        }
    }
}
