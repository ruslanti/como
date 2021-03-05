use std::convert::TryInto;

use anyhow::{anyhow, bail, ensure, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::Encoder;

use crate::v5::decoder::{decode_utf8_string, decode_variable_integer};
use crate::v5::encoder::RemainingLength;
use crate::v5::property::{PropertiesBuilder, PropertiesSize, Property, SubscribeProperties};
use crate::v5::types::{
    ControlPacket, MQTTCodec, MqttString, QoS, Retain, SubAck, Subscribe, SubscriptionOptions,
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
            let subscription_option = reader.get_u8();
            let qos: QoS = (subscription_option & 0b00000011).try_into()?;
            let nl = ((subscription_option & 0b00000100) >> 2) != 0;
            let rap = ((subscription_option & 0b00001000) >> 3) != 0;
            let retain: Retain = ((subscription_option & 0b00110000) >> 4).try_into()?;
            topic_filter.push((
                topic,
                SubscriptionOptions {
                    qos,
                    nl,
                    rap,
                    retain,
                },
            ))
        } else {
            bail!("empty topic filter");
        }
    }
    Ok(topic_filter)
}

impl RemainingLength for Subscribe {
    fn remaining_length(&self) -> usize {
        unimplemented!()
    }
}

impl Encoder<Subscribe> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, _item: Subscribe, _dst: &mut BytesMut) -> Result<(), Self::Error> {
        unimplemented!()
    }
}

impl RemainingLength for SubAck {
    fn remaining_length(&self) -> usize {
        let properties_length = self.properties.size();
        2 + properties_length.size()
            + properties_length
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
