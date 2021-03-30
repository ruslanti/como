use std::convert::TryInto;
use std::mem::size_of_val;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::Encoder;

use crate::v5::decoder::{decode_utf8_string, decode_variable_integer};
use crate::v5::encoder::encode_utf8_string;
use crate::v5::encoder::RemainingLength;
use crate::v5::error::MqttError;
use crate::v5::error::MqttError::{EndOfStream, UnacceptableProperty};
use crate::v5::property::*;
use crate::v5::types::{ConnAck, ControlPacket, MqttCodec};

pub fn decode_connack(mut reader: Bytes) -> Result<Option<ControlPacket>, MqttError> {
    end_of_stream!(reader.remaining() < 3, "connack flags");
    let flags = reader.get_u8();
    let session_present = (flags & 0b00000001) != 0;
    let reason_code = reader.get_u8().try_into()?;
    let properties_length = decode_variable_integer(&mut reader)? as usize;
    let properties = decode_connack_properties(reader.split_to(properties_length))?;
    Ok(Some(ControlPacket::ConnAck(ConnAck {
        session_present,
        reason_code,
        properties,
    })))
}

pub fn decode_connack_properties(mut reader: Bytes) -> Result<ConnAckProperties, MqttError> {
    let mut builder = PropertiesBuilder::default();
    while reader.has_remaining() {
        let id = decode_variable_integer(&mut reader)?;
        let property = id.try_into()?;
        match property {
            Property::SessionExpireInterval => {
                end_of_stream!(reader.remaining() < 4, "session expire interval");
                builder = builder.session_expire_interval(reader.get_u32())?;
            }
            Property::ReceiveMaximum => {
                end_of_stream!(reader.remaining() < 2, "receive maximum");
                builder = builder.receive_maximum(reader.get_u16())?;
            }
            Property::MaximumQoS => {
                end_of_stream!(reader.remaining() < 1, "maximum qos");
                builder = builder.maximum_qos(reader.get_u8())?;
            }
            Property::RetainAvailable => {
                end_of_stream!(reader.remaining() < 1, "retain available");
                builder = builder.retain_available(reader.get_u8())?;
            }
            Property::MaximumPacketSize => {
                end_of_stream!(reader.remaining() < 4, "maximum packet size");
                builder = builder.maximum_packet_size(reader.get_u32())?;
            }
            Property::AssignedClientIdentifier => {
                builder = builder.assigned_client_identifier(decode_utf8_string(&mut reader)?)?;
            }
            Property::TopicAliasMaximum => {
                end_of_stream!(reader.remaining() < 2, "topic alias maximum");
                builder = builder.topic_alias_maximum(reader.get_u16())?;
            }
            Property::ReasonString => {
                builder = builder.response_topic(decode_utf8_string(&mut reader)?)?;
            }
            Property::UserProperty => {
                let user_property = (
                    decode_utf8_string(&mut reader)?,
                    decode_utf8_string(&mut reader)?,
                );
                if let (Some(key), Some(value)) = user_property {
                    builder = builder.user_properties((key, value));
                }
            }
            Property::WildcardSubscriptionAvailable => {
                end_of_stream!(reader.remaining() < 1, "wildcard subscription available");
                builder = builder.wildcard_subscription_available(reader.get_u8())?;
            }
            Property::SubscriptionIdentifierAvailable => {
                end_of_stream!(reader.remaining() < 1, "subscription identifier available");
                builder = builder.subscription_identifier_available(reader.get_u8())?;
            }
            Property::SharedSubscriptionAvailable => {
                end_of_stream!(reader.remaining() < 1, "shared subscription available");
                builder = builder.shared_subscription_available(reader.get_u8())?;
            }
            Property::ServerKeepAlive => {
                end_of_stream!(reader.remaining() < 2, "server keep alive");
                builder = builder.server_keep_alive(reader.get_u16())?;
            }
            Property::AuthenticationMethod => unimplemented!(),
            Property::AuthenticationData => unimplemented!(),
            _ => return Err(UnacceptableProperty(property)),
        }
    }
    Ok(builder.connack())
}

impl Encoder<ConnAckProperties> for MqttCodec {
    type Error = MqttError;

    fn encode(
        &mut self,
        properties: ConnAckProperties,
        writer: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.encode(properties.size(), writer)?; // properties length

        encode_property_u32!(
            writer,
            SessionExpireInterval,
            properties.session_expire_interval
        );
        encode_property_u16!(writer, ReceiveMaximum, properties.receive_maximum);
        encode_property_u8!(writer, MaximumQoS, properties.maximum_qos.map(|q| q.into()));
        encode_property_u8!(
            writer,
            RetainAvailable,
            properties.retain_available.map(|b| b as u8)
        );
        encode_property_u32!(writer, MaximumPacketSize, properties.maximum_packet_size);
        encode_property_string!(
            writer,
            AssignedClientIdentifier,
            properties.assigned_client_identifier
        );
        encode_property_u16!(writer, TopicAliasMaximum, properties.topic_alias_maximum);
        encode_property_string!(writer, ReasonString, properties.reason_string);
        encode_property_user_properties!(writer, UserProperty, properties.user_properties);
        Ok(())
    }
}

impl Encoder<ConnAck> for MqttCodec {
    type Error = MqttError;

    fn encode(&mut self, msg: ConnAck, writer: &mut BytesMut) -> Result<(), Self::Error> {
        writer.put_u8(msg.session_present as u8); // connack flags
        writer.put_u8(msg.reason_code.into());
        self.encode(msg.properties, writer)
    }
}

impl RemainingLength for ConnAck {
    fn remaining_length(&self) -> usize {
        let properties_length = self.properties.size();
        2 + properties_length.size() + properties_length
    }
}

impl PropertiesSize for ConnAckProperties {
    fn size(&self) -> usize {
        let mut len = check_size_of!(self, session_expire_interval);
        len += check_size_of!(self, receive_maximum);
        len += check_size_of!(self, maximum_qos);
        len += check_size_of!(self, retain_available);
        len += check_size_of!(self, maximum_packet_size);
        len += check_size_of_string!(self, assigned_client_identifier);
        len += check_size_of!(self, topic_alias_maximum);
        len += check_size_of_string!(self, reason_string);
        len += self
            .user_properties
            .iter()
            .map(|(x, y)| 5 + x.len() + y.len())
            .sum::<usize>();
        len
    }
}
