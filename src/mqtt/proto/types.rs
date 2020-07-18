use core::fmt;
use std::collections::BTreeSet;
use tracing_subscriber::fmt::Formatter;
use std::convert::{TryFrom, TryInto};
use anyhow::{anyhow, Result};
use crate::mqtt::proto::property::{WillProperties, ConnectProperties, ConnAckProperties};
use bytes::{BytesMut, Bytes};

#[macro_use]
macro_rules! end_of_stream {
    ($condition: expr) => {
       if $condition {return Err(anyhow!("end of stream"))};
    };
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum QoS {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce
}

impl TryFrom<u8> for QoS {
    type Error = anyhow::Error;

    fn try_from(v: u8) -> Result<Self> {
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
            QoS::AtMostOnce  => 0,
            QoS::AtLeastOnce => 1,
            QoS::ExactlyOnce => 2,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum PacketType {
    CONNECT,
    CONNACK,
    PUBLISH{ dup: bool, qos: QoS, retain: bool },
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

    fn try_from(v: u8) -> Result<Self> {
        match v {
            0x10 => Ok(PacketType::CONNECT),
            0x20 => Ok(PacketType::CONNACK),
            0x30..=0x3F => Ok(PacketType::PUBLISH {dup: v&0b0000_1000 != 0, qos:((v&0b0000_0110)>>1).try_into()?, retain: v&0b0000_0001 != 0}),
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
            PacketType::CONNECT     => 0x10,
            PacketType::CONNACK     => 0x20,
            PacketType::PUBLISH{dup, qos, retain}
            => 0x30 | ((dup as u8) << 3) | ((qos as u8) << 1) | (retain as u8),
            PacketType::PUBACK      => 0x40,
            PacketType::PUBREC      => 0x50,
            PacketType::PUBREL      => 0x62,
            PacketType::PUBCOMP     => 0x70,
            PacketType::SUBSCRIBE   => 0x82,
            PacketType::SUBACK      => 0x90,
            PacketType::UNSUBSCRIBE => 0xA2,
            PacketType::UNSUBACK    => 0xB0,
            PacketType::PINGREQ     => 0xC0,
            PacketType::PINGRESP    => 0xD0,
            PacketType::DISCONNECT  => 0xE0,
            PacketType::AUTH        => 0xF0,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ReasonCode {
    Success                     = 0x00,
    UnspecifiedError            = 0x80,
    MalformedPacket             = 0x81,
    ProtocolError               = 0x82,
    ImplementationSpecificError = 0x83,
    UnsupportedProtocolVersion  = 0x84,
    ClientIdentifiersNotValid   = 0x85,
    BadUserNameOrPassword       = 0x86,
    NotAuthorized               = 0x87,
    ServerUnavailable           = 0x88,
    ServerBusy                  = 0x89,
    Banned                      = 0x8A,
    BadAuthenticationMethod     = 0x8C,
    TopicNameInvalid            = 0x90,
    PacketTooLarge              = 0x95,
    QuotaExceeded               = 0x97,
    PayloadFormatInvalid        = 0x99,
    RetainNotSupported          = 0x9A,
    QoSNotSupported             = 0x9B,
    UseAnotherServer            = 0x9C,
    ServerMoved                 = 0x9D,
    ConnectionRateExceeded      = 0x9F
}

impl TryFrom<u8> for ReasonCode {
    type Error = anyhow::Error;

    fn try_from(b: u8) -> Result<Self, Self::Error> {
        match b {
            0x00 => Ok(ReasonCode::Success),
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
            0x8C => Ok(ReasonCode::BadAuthenticationMethod),
            0x90 => Ok(ReasonCode::TopicNameInvalid),
            0x95 => Ok(ReasonCode::PacketTooLarge),
            0x97 => Ok(ReasonCode::QuotaExceeded),
            0x99 => Ok(ReasonCode::PayloadFormatInvalid),
            0x9A => Ok(ReasonCode::RetainNotSupported),
            0x9B => Ok(ReasonCode::QoSNotSupported),
            0x9C => Ok(ReasonCode::UseAnotherServer),
            0x9D => Ok(ReasonCode::ServerMoved),
            0x9F => Ok(ReasonCode::ConnectionRateExceeded),
            _ => Err(anyhow!("malformed control packet reason code: {}", b))
        }
    }
}

impl Into<u8> for ReasonCode {
    fn into(self) -> u8 {
        match self {
            ReasonCode::Success => 0x00,
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
            ReasonCode::BadAuthenticationMethod => 0x8C,
            ReasonCode::TopicNameInvalid => 0x90,
            ReasonCode::PacketTooLarge => 0x95,
            ReasonCode::QuotaExceeded => 0x97,
            ReasonCode::PayloadFormatInvalid => 0x99,
            ReasonCode::RetainNotSupported => 0x9A,
            ReasonCode::QoSNotSupported => 0x9B,
            ReasonCode::UseAnotherServer => 0x9C,
            ReasonCode::ServerMoved => 0x9D,
            ReasonCode::ConnectionRateExceeded => 0x9F
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Will {
    pub qos: QoS,
    pub retain: bool,
    pub properties: WillProperties,
    pub topic: Bytes,
    pub payload: Bytes
}

#[derive(Debug, Eq, PartialEq)]
pub enum ControlPacket {
    Connect {
        clean_start_flag: bool,
        keep_alive: u16,
        properties: ConnectProperties,
        client_identifier: Bytes,
        username: Option<Bytes>,
        password: Option<Bytes>,
        will: Option<Will>
    },
    ConnAck {
        session_present: bool,
        reason_code: ReasonCode,
        properties: ConnAckProperties
    },
    Publish,
    PubAck,
    PubRec,
    PubRel,
    Subscribe,
    SubAck,
    UnSubscribe,
    UnSubAck,
    PingReq,
    PingResp,
    Disconnect,
    Auth
}

pub enum PacketPart {
    FixedHeader,
    VariableHeader{remaining: usize, packet_type: PacketType},
    Payload
}

#[derive(Debug)]
pub struct MQTTCodec {
    pub part: PacketPart
}

impl fmt::Debug for PacketPart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PacketPart::FixedHeader => write!(f, "PacketPart::FixedHeader"),
            PacketPart::VariableHeader {
                remaining, packet_type
            } => f.debug_struct("PacketPart::VariableHeader")
                .field("remaining", &remaining)
                .field("packet_type", &packet_type)
                .finish(),
            PacketPart::Payload => write!(f, "PacketPart::Payload"),
        }
    }
}

impl MQTTCodec {
    pub fn new() -> MQTTCodec {
        MQTTCodec{part: PacketPart::FixedHeader}
    }
}

