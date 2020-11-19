use std::convert::TryInto;

use anyhow::{anyhow, bail, ensure, Result};
use bytes::{Buf, BufMut, BytesMut};

use crate::mqtt::proto::decoder::{decode_utf8_string, decode_variable_integer};
use crate::mqtt::proto::encoder::encode_utf8_string;
use crate::mqtt::proto::property::{DisconnectProperties, PropertiesBuilder, Property};
use crate::mqtt::proto::types::{ControlPacket, Disconnect, ReasonCode};

pub fn decode_disconnect(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    let reason_code = if reader.remaining() > 0 {
        reader.get_u8().try_into()?
    } else {
        ReasonCode::Success
    };
    let properties_length = if reader.remaining() > 0 {
        decode_variable_integer(reader)? as usize
    } else {
        0
    };
    let properties = decode_disconnect_properties(&mut reader.split_to(properties_length))?;
    Ok(Some(ControlPacket::Disconnect(Disconnect {
        reason_code,
        properties,
    })))
}

fn decode_disconnect_properties(reader: &mut BytesMut) -> Result<DisconnectProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(reader)?;
        match id.try_into()? {
            Property::SessionExpireInterval => {
                end_of_stream!(reader.remaining() < 4, "session expire interval");
                builder = builder.session_expire_interval(reader.get_u32())?;
            }
            Property::UserProperty => {
                let user_property = (decode_utf8_string(reader)?, decode_utf8_string(reader)?);
                if let (Some(key), Some(value)) = user_property {
                    builder = builder.user_properties((key, value));
                }
            }
            Property::ReasonString => {
                builder = builder.response_topic(decode_utf8_string(reader)?)?;
            }
            Property::ServerReference => {
                builder = builder.server_reference(decode_utf8_string(reader)?)?;
            }
            _ => bail!("unknown connect property: {:x}", id),
        }
    }
    Ok(builder.disconnect())
}

pub fn encode_disconnect_properties(
    writer: &mut BytesMut,
    properties: DisconnectProperties,
) -> Result<()> {
    encode_property_u32!(
        writer,
        SessionExpireInterval,
        properties.session_expire_interval
    );
    encode_property_string!(writer, ReasonString, properties.reason_string);
    encode_property_user_properties!(writer, UserProperty, properties.user_properties);
    encode_property_string!(writer, ServerReference, properties.server_reference);
    Ok(())
}
