use std::convert::TryInto;
use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut};
use crate::mqtt::proto::types::{ControlPacket, MqttString, UnSubscribe};
use crate::mqtt::proto::property::{PropertiesBuilder, Property, UnSubscribeProperties};
use crate::mqtt::proto::decoder::{decode_utf8_string, decode_variable_integer};

pub fn decode_unsubscribe(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    end_of_stream!(reader.remaining() < 2, "unsubscribe packet identifier");
    let packet_identifier = reader.get_u16();
    let properties_length = decode_variable_integer(reader)? as usize;
    let properties = decode_unsubscribe_properties(&mut reader.split_to(properties_length))?;
    let topic_filter = decode_unsubscribe_payload(reader)?;
    Ok(Some(ControlPacket::UnSubscribe(UnSubscribe {
        packet_identifier,
        properties,
        topic_filters: topic_filter
    })))
}

pub fn decode_unsubscribe_properties(reader: &mut BytesMut) -> Result<UnSubscribeProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(reader)?;
        match id.try_into()? {
            Property::UserProperty => {
                let user_property = (decode_utf8_string(reader)?, decode_utf8_string(reader)?);
                if let (Some(key), Some(value)) = user_property {
                    builder = builder.user_properties((key, value));
                }
            }
            _ => return Err(anyhow!("unknown unsubscribe property: {:x}", id))
        }
    }
    Ok(builder.unsubscribe())
}

pub fn decode_unsubscribe_payload(reader: &mut BytesMut) -> Result<Vec<MqttString>> {
    let mut topic_filter = vec![];
    while reader.has_remaining() {
        if let Some(topic) = decode_utf8_string(reader)? {
            topic_filter.push(topic)
        } else {
            return Err(anyhow!("empty topic filter"))
        }
    };
    Ok(topic_filter)
}
