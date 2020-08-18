use std::convert::TryInto;
use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut, BufMut};
use crate::mqtt::proto::types::{ControlPacket, QoS, Publish, Subscribe, SubOption, Retain, SubAck, MqttString};
use crate::mqtt::proto::property::{PropertiesBuilder, Property, PublishProperties, SubscribeProperties, SubAckProperties};
use crate::mqtt::proto::decoder::{decode_utf8_string, decode_variable_integer};
use crate::mqtt::proto::encoder::encode_utf8_string;

pub fn decode_subscribe(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    end_of_stream!(reader.remaining() < 2, "subscribe packet identifier");
    let packet_identifier = reader.get_u16();
    let properties_length = decode_variable_integer(reader)? as usize;
    let properties = decode_subscribe_properties(&mut reader.split_to(properties_length))?;
    let topic_filter = decode_subscribe_payload(reader)?;
    Ok(Some(ControlPacket::Subscribe(Subscribe {
        packet_identifier,
        properties,
        topic_filters: topic_filter
    })))
}

pub fn decode_subscribe_properties(reader: &mut BytesMut) -> Result<SubscribeProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(reader)?;
        match id.try_into()? {
            Property::SubscriptionIdentifier => {
                end_of_stream!(reader.remaining() < 4, "subscription identifier");
                builder = builder.subscription_identifier(decode_variable_integer(reader)?)?;
            },
            Property::UserProperty => {
                let user_property = (decode_utf8_string(reader)?, decode_utf8_string(reader)?);
                if let (Some(key), Some(value)) = user_property {
                    builder = builder.user_properties((key, value));
                }
            }
            _ => return Err(anyhow!("unknown subscribe property: {:x}", id))
        }
    }
    Ok(builder.subscribe())
}

pub fn decode_subscribe_payload(reader: &mut BytesMut) -> Result<Vec<(MqttString, SubOption)>> {
    let mut topic_filter = vec![];
    while reader.has_remaining() {
        if let Some(topic) = decode_utf8_string(reader)? {
            end_of_stream!(reader.remaining() < 1, "subscription option");
            let subscription_option = reader.get_u8();
            let qos: QoS = (subscription_option & 0b00000011).try_into()?;
            let nl = ((subscription_option & 0b00000100) >> 2) != 0;
            let rap = ((subscription_option & 0b00001000) >> 3) != 0;
            let retain: Retain = ((subscription_option & 0b00110000) >> 4).try_into()?;
            topic_filter.push((topic, SubOption{qos, nl, rap, retain}))
        } else {
            return Err(anyhow!("empty topic filter"))
        }
    };
    Ok(topic_filter)
}

pub fn encode_suback_properties(writer: &mut BytesMut, properties: SubAckProperties) -> Result<()> {
    encode_property_string!(writer, ReasonString, properties.reason_string);
    encode_property_user_properties!(writer, UserProperty, properties.user_properties);
    Ok(())
}