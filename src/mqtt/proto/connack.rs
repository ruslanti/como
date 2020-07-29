use std::convert::TryInto;
use anyhow::{anyhow, Result};
use bytes::{BytesMut, Buf, BufMut};
use crate::mqtt::proto::types::ControlPacket;
use crate::mqtt::proto::property::{PropertiesBuilder, Property, ConnAckProperties};
use crate::mqtt::proto::decoder::decode_variable_integer;
use crate::mqtt::proto::encoder::encode_utf8_string;

pub fn decode_connack(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    end_of_stream!(reader.remaining() < 3, "connack flags");
    let flags = reader.get_u8();
    let session_present = (flags & 0b00000001) != 0;
    let reason_code = reader.get_u8().try_into()?;
    let properties_length = decode_variable_integer(reader)? as usize;
    let properties = decode_connack_properties(&mut reader.split_to(properties_length))?;
    Ok(Some(ControlPacket::ConnAck {
        session_present,
        reason_code,
        properties
    }))
}

pub fn decode_connack_properties(reader: &mut BytesMut) -> Result<ConnAckProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(reader)?;
        match id.try_into()? {
            Property::SessionExpireInterval => {
                end_of_stream!(reader.remaining() < 4, "session expire interval");
                builder = builder.session_expire_interval(reader.get_u32())?;
            },
            Property::ReceiveMaximum => {
                end_of_stream!(reader.remaining() < 2, "receive maximum");
                builder = builder.receive_maximum(reader.get_u16())?;
            },
            Property::MaximumQoS => {
                end_of_stream!(reader.remaining() < 1, "maximum qos");
                builder = builder.maximum_qos(reader.get_u8())?;
            },
            Property::RetainAvailable => {
                end_of_stream!(reader.remaining() < 1, "retain available");
                builder = builder.retain_available(reader.get_u8())?;
            },
            Property::MaximumPacketSize => {
                end_of_stream!(reader.remaining() < 4, "maximum packet size");
                builder = builder.maximum_packet_size(reader.get_u32())?;
            },
            Property::AssignedClientIdentifier => {
                unimplemented!()
            },
            Property::TopicAliasMaximum => {
                end_of_stream!(reader.remaining() < 2, "topic alias maximum");
                builder = builder.topic_alias_maximum(reader.get_u16())?;
            }
            Property::ReasonString => {
                unimplemented!()
            },
            Property::UserProperty => {
                unimplemented!()
            },
            Property::WildcardSubscriptionAvailable => {
                end_of_stream!(reader.remaining() < 1, "wildcard subscription available");
                builder = builder.wildcard_subscription_available(reader.get_u8())?;
            },
            Property::SubscriptionIdentifierAvailable => {
                end_of_stream!(reader.remaining() < 1, "subscription identifier available");
                builder = builder.subscription_identifier_available(reader.get_u8())?;
            },
            Property::SharedSubscriptionAvailable => {
                end_of_stream!(reader.remaining() < 1, "shared subscription available");
                builder = builder.shared_subscription_available(reader.get_u8())?;
            },
            Property::ServerKeepAlive => {
                end_of_stream!(reader.remaining() < 2, "server keep alive");
                builder = builder.server_keep_alive(reader.get_u16())?;
            },
            Property::AuthenticationMethod => {
                unimplemented!()
            }
            Property::AuthenticationData => {
                unimplemented!()
            },
            _ => return Err(anyhow!("unknown connack property: {:x}", id))
        }
    }
    Ok(builder.connack())
}

pub fn encode_connack_properties(writer: &mut BytesMut, properties: ConnAckProperties) -> Result<()> {
    if let Some(value) = properties.session_expire_interval {
        end_of_stream!(writer.capacity() < 1, "session expire interval id");
        writer.put_u8(Property::SessionExpireInterval as u8);
        end_of_stream!(writer.capacity() < 4, "session expire interval");
        writer.put_u32(value);
    }
    if let Some(value) = properties.receive_maximum {
        end_of_stream!(writer.capacity() < 1, "receive maximum id");
        writer.put_u8(Property::ReceiveMaximum as u8);
        end_of_stream!(writer.capacity() < 2, "receive maximum");
        writer.put_u16(value);
    }
    if let Some(value) = properties.maximum_qos {
        end_of_stream!(writer.capacity() < 1, "maximum qos id");
        writer.put_u8(Property::MaximumQoS as u8);
        end_of_stream!(writer.capacity() < 1, "maximum qos");
        writer.put_u8(value.into());
    }
    if let Some(value) = properties.retain_available {
        end_of_stream!(writer.capacity() < 1, "retain available id");
        writer.put_u8(Property::RetainAvailable as u8);
        end_of_stream!(writer.capacity() < 1, "retain available");
        writer.put_u8(value as u8);
    }
    if let Some(value) = properties.maximum_packet_size {
        end_of_stream!(writer.capacity() < 1, "maximum packet size id");
        writer.put_u8(Property::MaximumPacketSize as u8);
        end_of_stream!(writer.capacity() < 4, "maximum packet size");
        writer.put_u32(value);
    }
    if let Some(value) = properties.assigned_client_identifier {
        end_of_stream!(writer.capacity() < 1, "assigned client identifier id");
        writer.put_u8(Property::AssignedClientIdentifier as u8);
        end_of_stream!(writer.capacity() < value.len(), "assigned client identifier");
        encode_utf8_string(writer, value)?;
    }
    if let Some(value) = properties.topic_alias_maximum {
        end_of_stream!(writer.capacity() < 1, "topic alias maximum id");
        writer.put_u8(Property::TopicAliasMaximum as u8);
        end_of_stream!(writer.capacity() < 2, "topic alias maximum");
        writer.put_u16(value);
    }
    if let Some(value) = properties.reason_string {
        end_of_stream!(writer.capacity() < 1, "reason string id");
        writer.put_u8(Property::ReasonString as u8);
        end_of_stream!(writer.capacity() < value.len(), "reason string");
        encode_utf8_string(writer, value)?;
    }
    for (first, second) in properties.user_properties {
        end_of_stream!(writer.capacity() < 1, "user properties");
        writer.put_u8(Property::UserProperty as u8);
        encode_utf8_string(writer, first)?;
        encode_utf8_string(writer, second)?;
    }
    Ok(())
}
