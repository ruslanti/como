use std::convert::{TryFrom, TryInto};

use anyhow::{anyhow, bail, ensure, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::Encoder;

use crate::v5::decoder::{decode_utf8_string, decode_variable_integer};
use crate::v5::encoder::{encode_utf8_string, RemainingLength};
use crate::v5::property::{PropertiesBuilder, PropertiesSize, Property, ResponseProperties};
use crate::v5::types::{MQTTCodec, PublishResponse, ReasonCode};

impl TryFrom<Bytes> for PublishResponse {
    type Error = anyhow::Error;

    fn try_from(mut reader: Bytes) -> Result<Self, Self::Error> {
        end_of_stream!(reader.remaining() < 2, "pubres variable header");
        let packet_identifier = reader.get_u16();
        let (reason_code, properties_length) = if reader.has_remaining() {
            (
                reader.get_u8().try_into()?,
                decode_variable_integer(&mut reader)? as usize,
            )
        } else {
            (ReasonCode::Success, 0)
        };
        let properties = ResponseProperties::try_from(reader.slice(..properties_length))?;

        Ok(PublishResponse {
            packet_identifier,
            reason_code,
            properties,
        })
    }
}

impl TryFrom<Bytes> for ResponseProperties {
    type Error = anyhow::Error;

    fn try_from(mut reader: Bytes) -> Result<Self, Self::Error> {
        let mut builder = PropertiesBuilder::default();
        while reader.has_remaining() {
            let id = decode_variable_integer(&mut reader)?;
            match id.try_into()? {
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
                _ => bail!("unknown pubres property: {:x}", id),
            }
        }
        Ok(builder.pubres())
    }
}

impl Encoder<PublishResponse> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, msg: PublishResponse, writer: &mut BytesMut) -> Result<(), Self::Error> {
        writer.put_u16(msg.packet_identifier); // packet identifier
        writer.put_u8(msg.reason_code.into());
        self.encode(msg.properties, writer)
    }
}

impl Encoder<ResponseProperties> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        properties: ResponseProperties,
        writer: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.encode(properties.size(), writer)?;
        // properties length
        encode_property_string!(writer, ReasonString, properties.reason_string);
        encode_property_user_properties!(writer, UserProperty, properties.user_properties);
        Ok(())
    }
}

impl RemainingLength for PublishResponse {
    fn remaining_length(&self) -> usize {
        let properties_length = self.properties.size();
        3 + properties_length.size() + properties_length
    }
}

impl PropertiesSize for ResponseProperties {
    fn size(&self) -> usize {
        let mut len = check_size_of_string!(self, reason_string);
        len += self
            .user_properties
            .iter()
            .map(|(x, y)| 5 + x.len() + y.len())
            .sum::<usize>();
        len
    }
}
