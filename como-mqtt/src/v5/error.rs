use std::io;
use std::string::FromUtf8Error;

use thiserror::Error;

use crate::v5::property::Property;
use crate::v5::types::{QoS, ReasonCode};

#[derive(Error, Debug)]
pub enum MqttError {
    #[error(transparent)]
    Io {
        #[from]
        source: io::Error,
    },
    #[error(transparent)]
    BincodeErrorKind {
        #[from]
        source: bincode::ErrorKind,
    },
    #[error(transparent)]
    FromUtf8Error {
        #[from]
        source: FromUtf8Error,
    },

    #[error("Unspecified error: {0}")]
    UnspecifiedError(String),

    #[error("End of stream: {0}")]
    EndOfStream(&'static str),
    #[error("Malformed control packet")]
    MalformedPacket,
    #[error("malformed variable integer: {0}")]
    MalformedVariableInteger(u32),
    #[error("Malformed control packet reason code: {0}")]
    MalformedReasonCode(u8),
    #[error("Malformed control packet type: {0}")]
    MalformedPacketType(u8),
    #[error("Malformed control packet QoS: {0}")]
    MalformedQoS(u8),
    #[error("Malformed control packet retain: {0}")]
    MalformedRetain(u8),
    #[error("Malformed control packet property type: {0}")]
    MalformedPropertyType(u32),

    #[error("Unacceptable property: {0}")]
    UnacceptableProperty(Property),
    #[error("Undefined packet identifier with qos: {0}")]
    UndefinedPacketIdentifier(QoS),
    #[error("More than once")]
    MoreThanOnceProperty,
    #[error("Packet too large")]
    PacketTooLarge,

    #[error("Implementation specific error")]
    ImplementationSpecificError,

    #[error("Unsupported protocol version: {0}")]
    UnsupportedProtocolVersion(String),

    #[error("Client Identifier not valid")]
    ClientIdentifierNotValid,

    #[error("Server unavailable")]
    ServerUnavailable,

    #[error("Server busy")]
    ServerBusy,

    #[error("Topic Filter invalid: {0}")]
    TopicFilterInvalid(String),

    #[error("Topic Name invalid: {0}")]
    TopicNameInvalid(String),

    #[error("Property {0} has empty value")]
    EmptyPropertyValue(&'static str),

    #[error("Not Authorized")]
    NotAuthorized,
    #[error("Bad username or password")]
    BadUserNameOrPassword,
}

impl From<MqttError> for ReasonCode {
    fn from(err: MqttError) -> Self {
        match err {
            MqttError::UnspecifiedError { .. } => ReasonCode::UnspecifiedError,
            MqttError::ImplementationSpecificError => ReasonCode::ImplementationSpecificError,
            MqttError::UnsupportedProtocolVersion(_) => ReasonCode::UnsupportedProtocolVersion,
            MqttError::ClientIdentifierNotValid => ReasonCode::ClientIdentifiersNotValid,
            MqttError::ServerUnavailable => ReasonCode::ServerUnavailable,
            MqttError::ServerBusy => ReasonCode::ServerBusy,
            MqttError::TopicFilterInvalid(_) => ReasonCode::TopicFilterInvalid,
            MqttError::TopicNameInvalid(_) => ReasonCode::TopicNameInvalid,
            MqttError::Io { .. } => ReasonCode::ImplementationSpecificError,
            MqttError::EndOfStream(_) => ReasonCode::MalformedPacket,
            MqttError::MalformedVariableInteger(_) => ReasonCode::MalformedPacket,
            MqttError::MalformedReasonCode(_) => ReasonCode::MalformedPacket,
            MqttError::MalformedPacketType(_) => ReasonCode::MalformedPacket,
            MqttError::MalformedQoS(_) => ReasonCode::MalformedPacket,
            MqttError::MalformedRetain(_) => ReasonCode::MalformedPacket,
            MqttError::MalformedPropertyType(_) => ReasonCode::MalformedPacket,
            MqttError::UnacceptableProperty(_) => ReasonCode::MalformedPacket,
            MqttError::UndefinedPacketIdentifier(_) => ReasonCode::ProtocolError,
            MqttError::MoreThanOnceProperty => ReasonCode::ProtocolError,
            MqttError::EmptyPropertyValue(_) => ReasonCode::ProtocolError,
            MqttError::BincodeErrorKind { .. } => ReasonCode::ImplementationSpecificError,
            MqttError::FromUtf8Error { .. } => ReasonCode::MalformedPacket,
            MqttError::PacketTooLarge => ReasonCode::PacketTooLarge,
            MqttError::NotAuthorized => ReasonCode::NotAuthorized,
            MqttError::BadUserNameOrPassword => ReasonCode::BadUserNameOrPassword,
            MqttError::MalformedPacket => ReasonCode::MalformedPacket,
        }
    }
}
