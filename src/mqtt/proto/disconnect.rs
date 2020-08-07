use bytes::{Buf, BytesMut, BufMut};
use anyhow::{anyhow, Result, Context};
use std::convert::TryInto;
use crate::mqtt::proto::types::{ControlPacket, ReasonCode, Disconnect};
use crate::mqtt::proto::decoder::{decode_variable_integer, decode_utf8_string};
use crate::mqtt::proto::property::{PropertiesBuilder, DisconnectProperties, Property};
use crate::mqtt::proto::encoder::encode_utf8_string;

pub fn decode_disconnect(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    let reason_code = if reader.remaining() > 0 { reader.get_u8().try_into()? } else { ReasonCode::Success };
    let properties_length = if reader.remaining() > 0 { decode_variable_integer(reader)? as usize } else { 0 };
    let properties = decode_disconnect_properties(&mut reader.split_to(properties_length))?;
    Ok(Some(ControlPacket::Disconnect(Disconnect {
        reason_code,
        properties
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

pub fn encode_disconnect_properties(writer: &mut BytesMut, properties: DisconnectProperties) -> Result<()> {
    if let Some(value) = properties.session_expire_interval {
        end_of_stream!(writer.capacity() < 1, "session expire interval id");
        writer.put_u8(Property::SessionExpireInterval as u8);
        end_of_stream!(writer.capacity() < 4, "session expire interval");
        writer.put_u32(value);
    }
    if let Some(value) = properties.reason_string {
        end_of_stream!(writer.capacity() < 1, "reason string id");
        writer.put_u8(Property::ReasonString as u8);
        end_of_stream!(writer.capacity() < value.len(), "reason string");
        encode_utf8_string(writer, value)?;
    }
    for (first, second) in properties.user_properties {
        end_of_stream!(writer.capacity() < 1, "user properties");
        writer.put_u8(Property::UserProperty as u8);
        encode_utf8_string(writer, first)?;
        encode_utf8_string(writer, second)?;
    }
    if let Some(value) = properties.server_reference {
        end_of_stream!(writer.capacity() < 1, "server reference");
        writer.put_u8(Property::ServerReference as u8);
        end_of_stream!(writer.capacity() < value.len(), "server reference");
        encode_utf8_string(writer, value)?;
    }
    Ok(())
}