use std::convert::TryInto;

use anyhow::{anyhow, bail, ensure, Result};
use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::Encoder;

use crate::mqtt::proto::decoder::{decode_utf8_string, decode_variable_integer};
use crate::mqtt::proto::encoder::encode_utf8_string;
use crate::mqtt::proto::property::*;
use crate::mqtt::proto::types::{ConnAck, ControlPacket, MQTTCodec};

pub fn decode_connack(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    end_of_stream!(reader.remaining() < 3, "connack flags");
    let flags = reader.get_u8();
    let session_present = (flags & 0b00000001) != 0;
    let reason_code = reader.get_u8().try_into()?;
    let properties_length = decode_variable_integer(reader)? as usize;
    let properties = decode_connack_properties(&mut reader.split_to(properties_length))?;
    Ok(Some(ControlPacket::ConnAck(ConnAck {
        session_present,
        reason_code,
        properties,
    })))
}

pub fn decode_connack_properties(reader: &mut BytesMut) -> Result<ConnAckProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(reader)?;
        match id.try_into()? {
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
            Property::AssignedClientIdentifier => unimplemented!(),
            Property::TopicAliasMaximum => {
                end_of_stream!(reader.remaining() < 2, "topic alias maximum");
                builder = builder.topic_alias_maximum(reader.get_u16())?;
            }
            Property::ReasonString => {
                builder = builder.response_topic(decode_utf8_string(reader)?)?;
            }
            Property::UserProperty => {
                let user_property = (decode_utf8_string(reader)?, decode_utf8_string(reader)?);
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
            _ => bail!("unknown connack property: {:x}", id),
        }
    }
    Ok(builder.connack())
}

impl Encoder<ConnAckProperties> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        properties: ConnAckProperties,
        writer: &mut BytesMut,
    ) -> Result<(), Self::Error> {
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
