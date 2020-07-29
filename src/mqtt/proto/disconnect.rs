use bytes::{Buf, BytesMut};
use anyhow::{anyhow, Result, Context};
use std::convert::TryInto;
use crate::mqtt::proto::types::{ControlPacket, ReasonCode};
use crate::mqtt::proto::decoder::{decode_variable_integer, decode_utf8_string};
use crate::mqtt::proto::property::{PropertiesBuilder, DisconnectProperties, Property};

pub fn decode_disconnect(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    let reason_code = if reader.remaining() > 0 { reader.get_u8().try_into()? } else { ReasonCode::Success };
    let properties_length = if reader.remaining() > 0 { decode_variable_integer(reader)? as usize } else { 0 };
    let properties = decode_disconnect_properties(&mut reader.split_to(properties_length))?;
    Ok(Some(ControlPacket::Disconnect {
        reason_code,
        properties
    }))
}

fn decode_disconnect_properties(reader: &mut BytesMut) -> Result<DisconnectProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(reader)?;
        match id.try_into()? {
            Property::SessionExpireInterval => {
                end_of_stream!(reader.remaining() < 4, "session expire interval");
                builder = builder.session_expire_interval(reader.get_u32())?;
            },
            Property::UserProperty => {
                unimplemented!()
            }
            Property::ReasonString => {
                builder = builder.response_topic(decode_utf8_string(reader)?)?;
            },
            Property::ServerReference => {
                builder = builder.server_reference(decode_utf8_string(reader)?)?;
            }
            _ => return Err(anyhow!("unknown connect property: {:x}", id)).context("decode disconnect properties")
        }
    }
    Ok(builder.disconnect())
}
