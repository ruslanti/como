use std::convert::{TryFrom, TryInto};

use anyhow::{anyhow, bail, ensure, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::Encoder;

use crate::v5::decoder::{decode_utf8_string, decode_variable_integer};
use crate::v5::encoder::encode_utf8_string;
use crate::v5::encoder::encode_variable_integer;
use crate::v5::encoder::RemainingLength;
use crate::v5::property::{
    PropertiesBuilder, PropertiesSize, Property, ResponseProperties, SubscribeProperties,
};
use crate::v5::types::{
    ControlPacket, MQTTCodec, MqttString, ReasonCode, SubAck, Subscribe, SubscriptionOptions,
};

pub fn decode_subscribe(mut reader: Bytes) -> Result<Option<ControlPacket>> {
    end_of_stream!(reader.remaining() < 2, "subscribe packet identifier");
    let packet_identifier = reader.get_u16();
    let properties_length = decode_variable_integer(&mut reader)? as usize;
    let properties = decode_subscribe_properties(reader.split_to(properties_length))?;
    let topic_filter = decode_subscribe_payload(reader)?;
    Ok(Some(ControlPacket::Subscribe(Subscribe {
        packet_identifier,
        properties,
        topic_filters: topic_filter,
    })))
}

pub fn decode_subscribe_properties(mut reader: Bytes) -> Result<SubscribeProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(&mut reader)?;
        match id.try_into()? {
            Property::SubscriptionIdentifier => {
                end_of_stream!(reader.remaining() < 4, "subscription identifier");
                builder = builder.subscription_identifier(decode_variable_integer(&mut reader)?)?;
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
            _ => bail!("unknown subscribe property: {:x}", id),
        }
    }
    Ok(builder.subscribe())
}

pub fn decode_subscribe_payload(
    mut reader: Bytes,
) -> Result<Vec<(MqttString, SubscriptionOptions)>> {
    let mut topic_filter = vec![];
    while reader.has_remaining() {
        if let Some(topic) = decode_utf8_string(&mut reader)? {
            end_of_stream!(reader.remaining() < 1, "subscription option");
            let subscription_option = SubscriptionOptions::try_from(reader.get_u8())?;
            topic_filter.push((topic, subscription_option))
        } else {
            bail!("empty topic filter");
        }
    }
    Ok(topic_filter)
}

impl RemainingLength for Subscribe {
    fn remaining_length(&self) -> usize {
        let len = self.properties.size();
        2 + len.size()
            + len
            + self
                .topic_filters
                .iter()
                .map(|(topic_filter, _)| 2 + topic_filter.len() + std::mem::size_of::<u8>())
                .sum::<usize>()
    }
}

impl PropertiesSize for SubscribeProperties {
    fn size(&self) -> usize {
        let mut len = 0;
        if let Some(id) = self.subscription_identifier {
            len += (id as usize).size();
        };
        len += self
            .user_properties
            .iter()
            .map(|(x, y)| 5 + x.len() + y.len())
            .sum::<usize>();
        len
    }
}

impl Encoder<Subscribe> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, msg: Subscribe, writer: &mut BytesMut) -> Result<(), Self::Error> {
        writer.put_u16(msg.packet_identifier);
        self.encode(msg.properties, writer)?;
        for (topic_filter, subscription_option) in msg.topic_filters {
            self.encode(topic_filter, writer)?;
            writer.put_u8(u8::from(subscription_option))
        }
        Ok(())
    }
}

impl Encoder<SubscribeProperties> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        properties: SubscribeProperties,
        writer: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.encode(properties.size(), writer)?;
        encode_property_variable_integer!(
            writer,
            SubscriptionIdentifier,
            properties.subscription_identifier
        );
        encode_property_user_properties!(writer, UserProperty, properties.user_properties);
        Ok(())
    }
}

impl RemainingLength for SubAck {
    fn remaining_length(&self) -> usize {
        let len = self.properties.size();
        2 + len.size()
            + len
            + self
                .reason_codes
                .iter()
                .map(|_r| std::mem::size_of::<u8>())
                .sum::<usize>()
    }
}

impl Encoder<SubAck> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, msg: SubAck, writer: &mut BytesMut) -> Result<(), Self::Error> {
        writer.put_u16(msg.packet_identifier);
        self.encode(msg.properties, writer)?;
        for reason_code in msg.reason_codes.into_iter() {
            writer.put_u8(reason_code.into())
        }
        Ok(())
    }
}

impl TryFrom<Bytes> for SubAck {
    type Error = anyhow::Error;

    fn try_from(mut reader: Bytes) -> Result<Self, Self::Error> {
        end_of_stream!(reader.remaining() < 2, "suback packet_identifier");
        let packet_identifier = reader.get_u16();
        let properties_length = decode_variable_integer(&mut reader)? as usize;
        let properties = ResponseProperties::try_from(reader.slice(..properties_length))?;
        let mut reason_codes = vec![];
        while reader.has_remaining() {
            reason_codes.push(ReasonCode::try_from(reader.get_u8())?)
        }
        Ok(SubAck {
            packet_identifier,
            properties,
            reason_codes,
        })
    }
}
