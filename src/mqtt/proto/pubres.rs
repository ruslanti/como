use anyhow::{anyhow, Result};
use bytes::{BytesMut, Buf, BufMut};
use crate::mqtt::proto::types::{ControlPacket, ReasonCode, PubAck, PubRec, PubRel};
use std::convert::TryInto;
use crate::mqtt::proto::decoder::decode_variable_integer;
use crate::mqtt::proto::property::{PropertiesBuilder, Property, PubResProperties};
use crate::mqtt::proto::encoder::encode_utf8_string;

pub fn decode_pubres(reader: &mut BytesMut) -> Result<(u16, ReasonCode, PubResProperties)> {
    end_of_stream!(reader.remaining() < 4, "pubres variable header");
    let packet_identifier = reader.get_u16();
    let reason_code = reader.get_u8().try_into()?;
    let properties_length = decode_variable_integer(reader)? as usize;
    let properties = decode_pubres_properties(&mut reader.split_to(properties_length))?;
    Ok((
        packet_identifier,
        reason_code,
        properties
    ))
}

pub fn decode_puback(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    let (packet_identifier, reason_code, properties) = decode_pubres(reader)?;
    Ok(Some(ControlPacket::PubAck(PubAck {
        packet_identifier,
        reason_code,
        properties
    })))
}

pub fn decode_pubrec(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    let (packet_identifier, reason_code, properties) = decode_pubres(reader)?;
    Ok(Some(ControlPacket::PubRec(PubRec {
        packet_identifier,
        reason_code,
        properties
    })))
}

pub fn decode_pubrel(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    let (packet_identifier, reason_code, properties) = decode_pubres(reader)?;
    Ok(Some(ControlPacket::PubRel(PubRel {
        packet_identifier,
        reason_code,
        properties
    })))
}

pub fn decode_pubres_properties(reader: &mut BytesMut) -> Result<PubResProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(reader)?;
        match id.try_into()? {
            Property::ReasonString => {
                unimplemented!()
            },
            Property::UserProperty => {
                unimplemented!()
            },
            _ => return Err(anyhow!("unknown pubres property: {:x}", id))
        }
    }
    Ok(builder.pubres())
}

pub fn encode_pubres_properties(writer: &mut BytesMut, properties: PubResProperties) -> Result<()> {
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
    Ok(())
}