use anyhow::{anyhow, Result};
use bytes::{BytesMut, Buf, BufMut};
use crate::mqtt::proto::types::{ControlPacket, ReasonCode, PubResp, PacketType};
use std::convert::TryInto;
use crate::mqtt::proto::decoder::decode_variable_integer;
use crate::mqtt::proto::property::{PropertiesBuilder, Property, PubResProperties, DisconnectProperties};
use crate::mqtt::proto::encoder::encode_utf8_string;
use tracing::{trace, debug, error, instrument};

pub fn decode_pubres(reader: &mut BytesMut) -> Result<(u16, ReasonCode, PubResProperties)> {
    end_of_stream!(reader.remaining() < 2, "pubres variable header");
    let packet_identifier = reader.get_u16();
    let (reason_code, properties_length) = if reader.has_remaining() {
            (reader.get_u8().try_into()?, decode_variable_integer(reader)? as usize)
        } else {
            (ReasonCode::Success, 0)
        };
    let properties = decode_pubres_properties(&mut reader.split_to(properties_length))?;
    Ok((
        packet_identifier,
        reason_code,
        properties
    ))
}

pub fn decode_puback(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    let (packet_identifier, reason_code, properties) = decode_pubres(reader)?;
    Ok(Some(ControlPacket::PubAck(PubResp {
        packet_type: PacketType::PUBACK,
        packet_identifier,
        reason_code,
        properties
    })))
}

pub fn decode_pubrec(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    let (packet_identifier, reason_code, properties) = decode_pubres(reader)?;
    Ok(Some(ControlPacket::PubRec(PubResp {
        packet_type: PacketType::PUBREC,
        packet_identifier,
        reason_code,
        properties
    })))
}

pub fn decode_pubrel(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    let (packet_identifier, reason_code, properties) = decode_pubres(reader)?;
    Ok(Some(ControlPacket::PubRel(PubResp {
        packet_type: PacketType::PUBREL,
        packet_identifier,
        reason_code,
        properties
    })))
}

pub fn decode_pubres_properties(reader: &mut BytesMut) -> Result<PubResProperties> {
    let builder = PropertiesBuilder::new();
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
    encode_property_string!(writer, ReasonString, properties.reason_string);
    encode_property_user_properties!(writer, UserProperty, properties.user_properties);
    Ok(())
}
