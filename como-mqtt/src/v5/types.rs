use core::fmt;
use std::convert::{TryFrom, TryInto};

use anyhow::{anyhow, Error};
use bytes::Bytes;
use serde::de::Visitor;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use crate::v5::property::{
    AuthProperties, ConnAckProperties, ConnectProperties, DisconnectProperties, PublishProperties,
    ResponseProperties, SubscribeProperties, UnSubscribeProperties, WillProperties,
};
use crate::v5::string::MqttString;

#[macro_use]
macro_rules! end_of_stream {
    ($condition: expr, $context: expr) => {
        ensure!(!$condition, anyhow!("end of stream").context($context));
    };
}

pub const MQTT: &str = "MQTT";
pub const VERSION: u8 = 5;

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

impl From<QoS> for u8 {
    fn from(qos: QoS) -> Self {
        match qos {
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

impl Serialize for QoS {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8((*self).into())
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash, Serialize, Deserialize)]
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

impl From<Retain> for u8 {
    fn from(value: Retain) -> Self {
        match value {
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
    GrantedQoS1 = 0x01,
    GrantedQoS2 = 0x02,
    DisconnectWithWill = 0x04,
    NoSubscriptionExisted = 0x11,
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
    SessionTakenOver = 0x8E,
    TopicFilterInvalid = 0x8F,
    TopicNameInvalid = 0x90,
    PacketIdentifierInUse = 0x91,
    PacketIdentifierNotFound = 0x92,
    ReceiveMaximumExceeded = 0x93,
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
            0x01 => Ok(ReasonCode::GrantedQoS1),
            0x02 => Ok(ReasonCode::GrantedQoS2),
            0x04 => Ok(ReasonCode::DisconnectWithWill),
            0x11 => Ok(ReasonCode::NoSubscriptionExisted),
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
            0x8E => Ok(ReasonCode::SessionTakenOver),
            0x8F => Ok(ReasonCode::TopicFilterInvalid),
            0x90 => Ok(ReasonCode::TopicNameInvalid),
            0x91 => Ok(ReasonCode::PacketIdentifierInUse),
            0x92 => Ok(ReasonCode::PacketIdentifierNotFound),
            0x93 => Ok(ReasonCode::ReceiveMaximumExceeded),
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
            ReasonCode::GrantedQoS1 => 0x01,
            ReasonCode::GrantedQoS2 => 0x02,
            ReasonCode::DisconnectWithWill => 0x04,
            ReasonCode::NoSubscriptionExisted => 0x11,
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
            ReasonCode::SessionTakenOver => 0x8E,
            ReasonCode::TopicFilterInvalid => 0x8F,
            ReasonCode::TopicNameInvalid => 0x90,
            ReasonCode::PacketIdentifierInUse => 0x91,
            ReasonCode::PacketIdentifierNotFound => 0x92,
            ReasonCode::ReceiveMaximumExceeded => 0x93,
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

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Will {
    pub qos: QoS,
    pub retain: bool,
    pub properties: WillProperties,
    pub topic: MqttString,
    pub payload: Bytes,
}

#[derive(Eq, PartialEq, Debug)]
pub struct Connect {
    pub clean_start_flag: bool,
    pub keep_alive: u16,
    pub properties: ConnectProperties,
    pub client_identifier: Option<MqttString>,
    pub username: Option<MqttString>,
    pub password: Option<MqttString>,
    pub will: Option<Will>,
}

#[derive(Eq, PartialEq, Debug)]
pub struct ConnAck {
    pub session_present: bool,
    pub reason_code: ReasonCode,
    pub properties: ConnAckProperties,
}

#[derive(Eq, PartialEq, Debug)]
pub struct Publish {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic_name: MqttString,
    pub packet_identifier: Option<u16>,
    pub properties: PublishProperties,
    pub payload: Bytes,
}

#[derive(Eq, PartialEq, Debug)]
pub struct PublishResponse {
    pub packet_identifier: u16,
    pub reason_code: ReasonCode,
    pub properties: ResponseProperties,
}

#[derive(Eq, PartialEq, Debug)]
pub struct Disconnect {
    pub reason_code: ReasonCode,
    pub properties: DisconnectProperties,
}

#[derive(Debug, Eq, PartialEq, Clone, Hash, Serialize, Deserialize)]
pub struct SubscriptionOptions {
    pub qos: QoS,
    pub nl: bool,
    pub rap: bool,
    pub retain: Retain,
}

#[derive(Eq, PartialEq, Debug)]
pub struct Subscribe {
    pub packet_identifier: u16,
    pub properties: SubscribeProperties,
    pub topic_filters: Vec<(MqttString, SubscriptionOptions)>,
}

#[derive(Eq, PartialEq, Debug)]
pub struct SubAck {
    pub packet_identifier: u16,
    pub properties: ResponseProperties,
    pub reason_codes: Vec<ReasonCode>,
}

#[derive(Eq, PartialEq, Debug)]
pub struct UnSubscribe {
    pub packet_identifier: u16,
    pub properties: UnSubscribeProperties,
    pub topic_filters: Vec<MqttString>,
}

#[derive(Eq, PartialEq, Debug)]
pub struct Auth {
    pub reason_code: ReasonCode,
    pub properties: AuthProperties,
}

#[derive(PartialEq, Debug)]
pub enum ControlPacket {
    Connect(Connect),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck(PublishResponse),
    PubRec(PublishResponse),
    PubRel(PublishResponse),
    PubComp(PublishResponse),
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

impl Connect {
    pub(crate) fn get_flags(&self) -> u8 {
        let mut flags = 0b0000_0000;
        flags |= (self.clean_start_flag as u8) << 1;
        if let Some(will) = &self.will {
            flags |= 0b0000_0100;
            flags |= u8::from(will.qos) << 3;
            flags |= (will.retain as u8) << 5;
        }
        flags |= (self.password.is_some() as u8) << 6;
        flags |= (self.username.is_some() as u8) << 6;
        flags
    }

    pub(crate) fn set_flags(flags: u8) -> anyhow::Result<(bool, bool, QoS, bool, bool, bool)> {
        let clean_start_flag = ((flags & 0b00000010) >> 1) != 0;
        let will_flag = ((flags & 0b00000100) >> 2) != 0;
        let will_qos_flag: QoS = QoS::try_from((flags & 0b00011000) >> 3)?;
        let will_retain_flag = ((flags & 0b00100000) >> 5) != 0;
        let password_flag = ((flags & 0b01000000) >> 6) != 0;
        let username_flag = ((flags & 0b10000000) >> 7) != 0;
        Ok((
            clean_start_flag,
            will_flag,
            will_qos_flag,
            will_retain_flag,
            password_flag,
            username_flag,
        ))
    }
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

impl Into<&str> for &ControlPacket {
    fn into(self) -> &'static str {
        match self {
            ControlPacket::Connect(_) => "CONNECT",
            ControlPacket::ConnAck(_) => "CONNACK",
            ControlPacket::Publish(_) => "PUBLISH",
            ControlPacket::PubAck(_) => "PUBACK",
            ControlPacket::PubRec(_) => "PUBREC",
            ControlPacket::PubRel(_) => "PUBREL",
            ControlPacket::PubComp(_) => "PUBCOMP",
            ControlPacket::Subscribe(_) => "SUBSCRIBE",
            ControlPacket::SubAck(_) => "SUBACK",
            ControlPacket::UnSubscribe(_) => "UNSUBSCRIBE",
            ControlPacket::UnSubAck(_) => "UNSUBACK",
            ControlPacket::PingReq => "PINGREQ",
            ControlPacket::PingResp => "PINGRESP",
            ControlPacket::Disconnect(_) => "DISCONNECT",
            ControlPacket::Auth(_) => "AUTH",
        }
    }
}

impl fmt::Display for ControlPacket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ControlPacket::Connect(m) => write!(f, "{}", m),
            ControlPacket::ConnAck(m) => write!(f, "{}", m),
            ControlPacket::Publish(m) => write!(f, "{}", m),
            ControlPacket::PubAck(m) => write!(f, "PUBACK {}", m),
            ControlPacket::PubRec(m) => write!(f, "PUBREC {}", m),
            ControlPacket::PubRel(m) => write!(f, "PUBREL {}", m),
            ControlPacket::PubComp(m) => write!(f, "PUBCOMP {}", m),
            ControlPacket::Subscribe(m) => write!(f, "{}", m),
            ControlPacket::SubAck(m) => write!(f, "SUBACK {}", m),
            ControlPacket::UnSubscribe(m) => write!(f, "{}", m),
            ControlPacket::UnSubAck(m) => write!(f, "UNSUBACK {}", m),
            ControlPacket::PingReq => write!(f, "PINGREQ"),
            ControlPacket::PingResp => write!(f, "PINGRESP"),
            ControlPacket::Disconnect(m) => write!(f, "{}", m),
            ControlPacket::Auth(m) => write!(f, "{}", m),
        }
    }
}

macro_rules! debug_field {
    ($self:ident, $writer:ident, $property:ident) => {
        if let Some(value) = &$self.$property {
            let _ = $writer.field(stringify!($property), value);
        }
    };
}

impl fmt::Display for Connect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("CONNECT");
        debug_struct.field("clean_start", &self.clean_start_flag);
        debug_struct.field("keep_alive", &self.keep_alive);
        debug_field!(self, debug_struct, client_identifier);
        //debug_struct.field("properties", &self.properties);
        debug_struct.finish()
    }
}

impl fmt::Display for ConnAck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("CONNACK");
        debug_struct.field("session_present", &self.session_present);
        debug_struct.field("reason_code", &self.reason_code);
        debug_struct.finish()
    }
}

impl fmt::Display for Publish {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("PUBLISH");
        debug_struct.field("dup", &self.dup);
        debug_struct.field("qos", &self.qos);
        debug_struct.field("retain", &self.retain);
        debug_field!(self, debug_struct, packet_identifier);
        debug_struct.field("topic_name", &self.topic_name);
        debug_struct.field("payload", &self.payload);
        debug_struct.finish()
    }
}

impl fmt::Display for PublishResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("");
        debug_struct.field("reason_code", &self.reason_code);
        debug_struct.finish()
    }
}

impl fmt::Display for Disconnect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("DISCONNECT");
        debug_struct.field("reason_code", &self.reason_code);
        debug_struct.finish()
    }
}

impl fmt::Display for Subscribe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("SUBSCRIBE");
        debug_struct.field("packet_identifier", &self.packet_identifier);
        debug_struct.field("topic_filters", &self.topic_filters);
        debug_struct.finish()
    }
}

impl fmt::Display for SubAck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("");
        debug_struct.field("reason_codes", &self.reason_codes);
        debug_struct.finish()
    }
}

impl fmt::Display for UnSubscribe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("UNSUBSCRIBE");
        debug_struct.field("topic_filters", &self.topic_filters);
        debug_struct.finish()
    }
}

impl fmt::Display for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("AUTH");
        debug_struct.finish()
    }
}

impl TryFrom<u8> for SubscriptionOptions {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let qos: QoS = (value & 0b00000011).try_into()?;
        let nl = ((value & 0b00000100) >> 2) != 0;
        let rap = ((value & 0b00001000) >> 3) != 0;
        let retain: Retain = ((value & 0b00110000) >> 4).try_into()?;
        Ok(SubscriptionOptions {
            qos,
            nl,
            rap,
            retain,
        })
    }
}

impl From<SubscriptionOptions> for u8 {
    fn from(value: SubscriptionOptions) -> Self {
        let mut option = 0b0000_0000;
        option |= u8::from(value.qos);
        option |= (value.nl as u8) << 2;
        option |= (value.rap as u8) << 3;
        option |= u8::from(value.retain) << 4;
        option
    }
}

impl TryFrom<&[u8]> for SubscriptionOptions {
    type Error = Error;

    fn try_from(encoded: &[u8]) -> Result<Self, Self::Error> {
        bincode::deserialize(encoded).map_err(Error::msg)
    }
}

impl Default for MQTTCodec {
    fn default() -> Self {
        MQTTCodec {
            part: PacketPart::FixedHeader,
        }
    }
}
