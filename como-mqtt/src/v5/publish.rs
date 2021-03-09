use std::convert::TryInto;
use std::mem::size_of_val;

use anyhow::{anyhow, bail, ensure, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::Encoder;

use crate::v5::decoder::{decode_utf8_string, decode_variable_integer};
use crate::v5::encoder::encode_utf8_string;
use crate::v5::encoder::encode_variable_integer;
use crate::v5::encoder::RemainingLength;
use crate::v5::property::{PropertiesBuilder, PropertiesSize, Property, PublishProperties};
use crate::v5::types::{ControlPacket, MQTTCodec, Publish, QoS};

pub fn decode_publish(
    dup: bool,
    qos: QoS,
    retain: bool,
    mut reader: Bytes,
) -> Result<Option<ControlPacket>> {
    end_of_stream!(reader.remaining() < 3, "publish topic name");
    let topic_name = match decode_utf8_string(&mut reader)? {
        Some(v) => v,
        None => bail!("missing topic in publish message"),
    };
    let packet_identifier = if qos == QoS::AtMostOnce {
        None
    } else {
        end_of_stream!(reader.remaining() < 2, "publish packet identifier");
        Some(reader.get_u16())
    };
    let properties_length = decode_variable_integer(&mut reader)? as usize;
    let properties = decode_publish_properties(reader.split_to(properties_length))?;
    Ok(Some(ControlPacket::Publish(Publish {
        dup,
        qos,
        retain,
        topic_name,
        packet_identifier,
        properties,
        payload: reader,
    })))
}

pub fn decode_publish_properties(mut reader: Bytes) -> Result<PublishProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(&mut reader)?;
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
                builder = builder.content_type(decode_utf8_string(&mut reader)?)?;
            }
            Property::ResponseTopic => {
                builder = builder.response_topic(decode_utf8_string(&mut reader)?)?;
            }
            Property::CorrelationData => unimplemented!(),
            Property::UserProperty => {
                let user_property = (
                    decode_utf8_string(&mut reader)?,
                    decode_utf8_string(&mut reader)?,
                );
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
                builder = builder.subscription_identifier(decode_variable_integer(&mut reader)?)?;
            }
            _ => bail!("unknown publish property: {:x}", id),
        }
    }
    Ok(builder.publish())
}

impl Encoder<Publish> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, msg: Publish, writer: &mut BytesMut) -> Result<(), Self::Error> {
        self.encode(msg.topic_name, writer)?;
        if QoS::AtMostOnce != msg.qos {
            if let Some(packet_identifier) = msg.packet_identifier {
                writer.put_u16(packet_identifier); // packet identifier
            } else {
                return Err(anyhow!(
                    "undefined packet identifier with qos: {:?}",
                    msg.qos
                ));
            }
        }
        self.encode(msg.properties, writer)?;
        self.encode(msg.payload, writer)
    }
}

impl Publish {
    pub fn size(&self) -> usize {
        let len = self.remaining_length();
        1 + len.size() + len
    }
}

impl RemainingLength for Publish {
    fn remaining_length(&self) -> usize {
        let packet_identifier_len = if self.packet_identifier.is_some() {
            2
        } else {
            0
        };
        let properties_length = self.properties.size(); // properties

        self.topic_name.len()
            + 2
            + packet_identifier_len
            + properties_length.size()
            + properties_length
            + self.payload.len()
            + 2
    }
}

impl PropertiesSize for PublishProperties {
    fn size(&self) -> usize {
        let mut len = check_size_of!(self, payload_format_indicator);
        len += check_size_of!(self, message_expire_interval);
        len += check_size_of!(self, topic_alias);
        len += check_size_of_string!(self, response_topic);
        len += check_size_of!(self, correlation_data);
        len += self
            .user_properties
            .iter()
            .map(|(x, y)| 5 + x.len() + y.len())
            .sum::<usize>();
        if let Some(id) = self.subscription_identifier {
            len += (id as usize).size();
        };
        len += check_size_of_string!(self, content_type);
        len
    }
}

impl Encoder<PublishProperties> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        properties: PublishProperties,
        writer: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        println!("encode properties size: {}", properties.size());
        self.encode(properties.size(), writer)?;
        // properties length
        encode_property_u8!(
            writer,
            PayloadFormatIndicator,
            properties.payload_format_indicator.map(|b| b as u8)
        );
        encode_property_u32!(
            writer,
            MessageExpireInterval,
            properties.message_expire_interval
        );
        encode_property_u16!(writer, TopicAlias, properties.topic_alias);
        encode_property_string!(writer, ResponseTopic, properties.response_topic);
        if properties.correlation_data.is_some() {
            unimplemented!()
        }
        encode_property_user_properties!(writer, UserProperty, properties.user_properties);
        encode_property_variable_integer!(
            writer,
            SubscriptionIdentifier,
            properties.subscription_identifier
        );
        encode_property_string!(writer, ContentType, properties.content_type);
        Ok(())
    }
}
