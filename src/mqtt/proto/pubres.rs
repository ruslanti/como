use std::convert::TryInto;

use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, BytesMut};

use crate::mqtt::proto::decoder::{decode_utf8_string, decode_variable_integer};
use crate::mqtt::proto::encoder::encode_utf8_string;
use crate::mqtt::proto::property::{PropertiesBuilder, Property, PubResProperties};
use crate::mqtt::proto::types::{ControlPacket, PacketType, PubResp, ReasonCode};

pub fn decode_pubres(reader: &mut BytesMut) -> Result<(u16, ReasonCode, PubResProperties)> {
    end_of_stream!(reader.remaining() < 2, "pubres variable header");
    let packet_identifier = reader.get_u16();
    let (reason_code, properties_length) = if reader.has_remaining() {
        (
            reader.get_u8().try_into()?,
            decode_variable_integer(reader)? as usize,
        )
    } else {
        (ReasonCode::Success, 0)
    };
    let properties = decode_pubres_properties(&mut reader.split_to(properties_length))?;
    Ok((packet_identifier, reason_code, properties))
}

pub fn decode_puback(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    let (packet_identifier, reason_code, properties) = decode_pubres(reader)?;
    Ok(Some(ControlPacket::PubAck(PubResp {
        packet_type: PacketType::PUBACK,
        packet_identifier,
        reason_code,
        properties,
    })))
}

pub fn decode_pubrec(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    let (packet_identifier, reason_code, properties) = decode_pubres(reader)?;
    Ok(Some(ControlPacket::PubRec(PubResp {
        packet_type: PacketType::PUBREC,
        packet_identifier,
        reason_code,
        properties,
    })))
}

pub fn decode_pubrel(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    let (packet_identifier, reason_code, properties) = decode_pubres(reader)?;
    Ok(Some(ControlPacket::PubRel(PubResp {
        packet_type: PacketType::PUBREL,
        packet_identifier,
        reason_code,
        properties,
    })))
}

pub fn decode_pubres_properties(reader: &mut BytesMut) -> Result<PubResProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(reader)?;
        match id.try_into()? {
            Property::ReasonString => {
                builder = builder.response_topic(decode_utf8_string(reader)?)?;
            }
            Property::UserProperty => {
                let user_property = (decode_utf8_string(reader)?, decode_utf8_string(reader)?);
                if let (Some(key), Some(value)) = user_property {
                    builder = builder.user_properties((key, value));
                }
            }
            _ => return Err(anyhow!("unknown pubres property: {:x}", id)),
        }
    }
    Ok(builder.pubres())
}

pub fn encode_pubres_properties(writer: &mut BytesMut, properties: PubResProperties) -> Result<()> {
    encode_property_string!(writer, ReasonString, properties.reason_string);
    encode_property_user_properties!(writer, UserProperty, properties.user_properties);
    Ok(())
}
