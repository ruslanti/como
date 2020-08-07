use std::convert::TryInto;
use anyhow::{anyhow, Result};
use bytes::{BytesMut, Buf};
use crate::mqtt::proto::property::{PropertiesBuilder, Property, WillProperties};
use crate::mqtt::proto::decoder::{decode_utf8_string, decode_variable_integer};

pub fn decode_will_properties(reader: &mut BytesMut) -> Result<WillProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(reader)?;
        match id.try_into()? {
            Property::WillDelayInterval => {
                end_of_stream!(reader.remaining() < 4, "will delay interval");
                builder = builder.will_delay_interval(reader.get_u32())?;
            },
            Property::PayloadFormatIndicator => {
                end_of_stream!(reader.remaining() < 1, "will payload format indicator");
                builder = builder.payload_format_indicator(reader.get_u8())?;
            },
            Property::MessageExpireInterval => {
                end_of_stream!(reader.remaining() < 4, "will message expire interval");
                builder = builder.message_expire_interval(reader.get_u32())?;
            },
            Property::ContentType => {
                builder = builder.content_type(decode_utf8_string(reader)?)?;
            }
            Property::ResponseTopic => {
                builder = builder.response_topic(decode_utf8_string(reader)?)?;
            },
            Property::CorrelationData => {
                unimplemented!()
            },
            Property::UserProperty => {
                unimplemented!()
            }
            _ => return Err(anyhow!("unknown will property: {:x}", id))
        }
    }
    Ok(builder.will())
}