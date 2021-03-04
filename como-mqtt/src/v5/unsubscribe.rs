use std::convert::TryInto;

use anyhow::{anyhow, bail, ensure, Result};
use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Encoder;

use crate::v5::decoder::{decode_utf8_string, decode_variable_integer};
use crate::v5::encoder::RemainingLength;
use crate::v5::property::{PropertiesBuilder, Property, UnSubscribeProperties};
use crate::v5::types::{ControlPacket, MQTTCodec, MqttString, UnSubscribe};

pub fn decode_unsubscribe(mut reader: Bytes) -> Result<Option<ControlPacket>> {
    end_of_stream!(reader.remaining() < 2, "unsubscribe packet identifier");
    let packet_identifier = reader.get_u16();
    let properties_length = decode_variable_integer(&mut reader)? as usize;
    let properties = decode_unsubscribe_properties(reader.split_to(properties_length))?;
    let topic_filter = decode_unsubscribe_payload(reader)?;
    Ok(Some(ControlPacket::UnSubscribe(UnSubscribe {
        packet_identifier,
        properties,
        topic_filters: topic_filter,
    })))
}

pub fn decode_unsubscribe_properties(mut reader: Bytes) -> Result<UnSubscribeProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(&mut reader)?;
        match id.try_into()? {
            Property::UserProperty => {
                let user_property = (
                    decode_utf8_string(&mut reader)?,
                    decode_utf8_string(&mut reader)?,
                );
                if let (Some(key), Some(value)) = user_property {
                    builder = builder.user_properties((key, value));
                }
            }
            _ => bail!("unknown unsubscribe property: {:x}", id),
        }
    }
    Ok(builder.unsubscribe())
}

pub fn decode_unsubscribe_payload(mut reader: Bytes) -> Result<Vec<MqttString>> {
    let mut topic_filter = vec![];
    while reader.has_remaining() {
        if let Some(topic) = decode_utf8_string(&mut reader)? {
            topic_filter.push(topic)
        } else {
            bail!("empty topic filter");
        }
    }
    Ok(topic_filter)
}

impl Encoder<UnSubscribe> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, _item: UnSubscribe, _dst: &mut BytesMut) -> Result<(), Self::Error> {
        unimplemented!()
    }
}

impl RemainingLength for UnSubscribe {
    fn remaining_length(&self) -> usize {
        unimplemented!()
    }
}
