use std::convert::TryInto;

use anyhow::{anyhow, bail, ensure, Result};
use bytes::{Buf, Bytes};

use crate::mqtt::proto::decoder::{decode_utf8_string, decode_variable_integer};
use crate::mqtt::proto::property::{PropertiesBuilder, Property, WillProperties};

pub fn decode_will_properties(mut reader: Bytes) -> Result<WillProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(&mut reader)?;
        match id.try_into()? {
            Property::WillDelayInterval => {
                end_of_stream!(reader.remaining() < 4, "will delay interval");
                builder = builder.will_delay_interval(reader.get_u32())?;
            }
            Property::PayloadFormatIndicator => {
                end_of_stream!(reader.remaining() < 1, "will payload format indicator");
                builder = builder.payload_format_indicator(reader.get_u8())?;
            }
            Property::MessageExpireInterval => {
                end_of_stream!(reader.remaining() < 4, "will message expire interval");
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
            _ => bail!("unknown will property: {:x}", id),
        }
    }
    Ok(builder.will())
}
