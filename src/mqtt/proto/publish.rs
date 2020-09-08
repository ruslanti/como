use std::convert::TryInto;

use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut};

use crate::mqtt::proto::decoder::{decode_utf8_string, decode_variable_integer};
use crate::mqtt::proto::property::{PropertiesBuilder, Property, PublishProperties};
use crate::mqtt::proto::types::{ControlPacket, Publish, QoS};

pub fn decode_publish(
    dup: bool,
    qos: QoS,
    retain: bool,
    reader: &mut BytesMut,
) -> Result<Option<ControlPacket>> {
    end_of_stream!(reader.remaining() < 3, "publish topic name");
    let topic_name = match decode_utf8_string(reader)? {
        Some(v) => v,
        None => return Err(anyhow!("missing topic in publish message")),
    };
    let packet_identifier = if qos == QoS::AtMostOnce {
        None
    } else {
        end_of_stream!(reader.remaining() < 2, "publish packet identifier");
        Some(reader.get_u16())
    };
    let properties_length = decode_variable_integer(reader)? as usize;
    let properties = decode_publish_properties(&mut reader.split_to(properties_length))?;
    Ok(Some(ControlPacket::Publish(Publish {
        dup,
        qos,
        retain,
        topic_name,
        packet_identifier,
        properties,
        payload: reader.to_bytes(),
    })))
}

pub fn decode_publish_properties(reader: &mut BytesMut) -> Result<PublishProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(reader)?;
        match id.try_into()? {
            Property::PayloadFormatIndicator => {
                end_of_stream!(reader.remaining() < 1, "payload format indicator");
                builder = builder.payload_format_indicator(reader.get_u8())?;
            }
            Property::MessageExpireInterval => {
                end_of_stream!(reader.remaining() < 4, "message expire interval");
                builder = builder.message_expire_interval(reader.get_u32())?;
            }
            Property::ContentType => {
                builder = builder.content_type(decode_utf8_string(reader)?)?;
            }
            Property::ResponseTopic => {
                builder = builder.response_topic(decode_utf8_string(reader)?)?;
            }
            Property::CorrelationData => unimplemented!(),
            Property::UserProperty => {
                let user_property = (decode_utf8_string(reader)?, decode_utf8_string(reader)?);
                if let (Some(key), Some(value)) = user_property {
                    builder = builder.user_properties((key, value));
                }
            }
            Property::TopicAlias => {
                end_of_stream!(reader.remaining() < 2, "topic alias");
                builder = builder.topic_alias(reader.get_u16())?;
            }
            Property::SubscriptionIdentifier => {
                end_of_stream!(reader.remaining() < 4, "subscription identifier");
                builder = builder.subscription_identifier(decode_variable_integer(reader)?)?;
            }
            _ => return Err(anyhow!("unknown publish property: {:x}", id)),
        }
    }
    Ok(builder.publish())
}
