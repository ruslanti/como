use crate::mqtt::proto::types::{MQTTCodec, ControlPacket, PacketType};
use tokio_util::codec::Encoder;
use bytes::{BytesMut, BufMut, Buf, Bytes};
use tokio::io::Error;
use anyhow::{anyhow, Result, Context};
use crate::mqtt::proto::property::{PropertiesLength, ConnAckProperties, Property};

impl Encoder<ControlPacket> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, packet: ControlPacket, writer: &mut BytesMut) -> Result<(), Self::Error> {
        match packet {
            ControlPacket::Connect { .. } => {},
            ControlPacket::ConnAck {
                session_present,
                reason_code,
                properties
            } => {
                let properties_length = properties.len(); // properties
                let properties_length_size= encoded_variable_integer_len(properties_length); // properties length
                let remaining_length = 2 + properties_length_size + properties_length;
                let remaining_length_size = encoded_variable_integer_len(remaining_length); // remaining length size
                writer.reserve(1 + remaining_length_size + remaining_length);

                writer.put_u8(PacketType::CONNACK.into()); // packet type
                encode_variable_integer(writer, remaining_length); // remaining length
                writer.put_u8(session_present as u8); // connack flags
                writer.put_u8(reason_code.into());
                encode_variable_integer(writer, properties_length); // properties length
                end_of_stream!(writer.remaining() < properties_length);
                encode_connack_properties(writer, properties);
            },
            ControlPacket::Publish => {},
            ControlPacket::PubAck => {},
            ControlPacket::PubRec => {},
            ControlPacket::PubRel => {},
            ControlPacket::Subscribe => {},
            ControlPacket::SubAck => {},
            ControlPacket::UnSubscribe => {},
            ControlPacket::UnSubAck => {},
            ControlPacket::PingReq => {},
            ControlPacket::PingResp => {},
            ControlPacket::Disconnect => {},
            ControlPacket::Auth => {},
        };
        Ok(())
    }
}

fn encode_connack_properties(writer: &mut BytesMut, properties: ConnAckProperties) -> Result<()> {
    if let Some(value) = properties.session_expire_interval {
        end_of_stream!(writer.remaining() < 1);
        writer.put_u8(Property::SessionExpireInterval as u8);
        end_of_stream!(writer.remaining() < 4);
        writer.put_u32(value);
    }
    if let Some(value) = properties.receive_maximum {
        end_of_stream!(writer.remaining() < 1);
        writer.put_u8(Property::ReceiveMaximum as u8);
        end_of_stream!(writer.remaining() < 2);
        writer.put_u16(value);
    }
    if let Some(value) = properties.maximum_qos {
        end_of_stream!(writer.remaining() < 1);
        writer.put_u8(Property::MaximumQoS as u8);
        end_of_stream!(writer.remaining() < 1);
        writer.put_u8(value.into());
    }
    if let Some(value) = properties.retain_available {
        end_of_stream!(writer.remaining() < 1);
        writer.put_u8(Property::RetainAvailable as u8);
        end_of_stream!(writer.remaining() < 1);
        writer.put_u8(value as u8);
    }
    if let Some(value) = properties.maximum_packet_size {
        end_of_stream!(writer.remaining() < 1);
        writer.put_u8(Property::MaximumPacketSize as u8);
        end_of_stream!(writer.remaining() < 4);
        writer.put_u32(value);
    }
    if let Some(value) = properties.assigned_client_identifier {
        end_of_stream!(writer.remaining() < 1);
        writer.put_u8(Property::AssignedClientIdentifier as u8);
        end_of_stream!(writer.remaining() < value.len());
        writer.put(value);
    }
    if let Some(value) = properties.topic_alias_maximum {
        end_of_stream!(writer.remaining() < 1);
        writer.put_u8(Property::TopicAliasMaximum as u8);
        end_of_stream!(writer.remaining() < 2);
        writer.put_u16(value);
    }
    if let Some(value) = properties.reason_string {
        end_of_stream!(writer.remaining() < 1);
        writer.put_u8(Property::ReasonString as u8);
        end_of_stream!(writer.remaining() < value.len());
        writer.put(value);
    }
    for (first, second) in properties.user_properties {
        end_of_stream!(writer.remaining() < 1);
        writer.put_u8(Property::UserProperty as u8);
        encode_utf8_string(writer, first)?;
        encode_utf8_string(writer, second)?;
    }
    Ok(())
}

fn encode_utf8_string(writer: &mut BytesMut, s: Bytes) -> Result<()> {
    end_of_stream!(writer.remaining() < 2);
    writer.put_u16(s.len() as u16);
    end_of_stream!(writer.remaining() < s.len());
    writer.put(s);
    Ok(())
}

fn encode_variable_integer(writer: &mut BytesMut, v: usize) -> Result<()> {
    let mut value = v;
    loop {
        let mut encoded_byte = (value % 0x80) as u8;
        value = value / 0x80;
        // if there are more data to encode, set the top bit of this byte
        if value > 0 {
            encoded_byte = encoded_byte | 0x80;
        }
        if writer.remaining() < 1 { return Err(anyhow!("end of stream")).context("encode_variable_integer") }
        writer.put_u8(encoded_byte);
        if value == 0 {
            break;
        }
    }
    Ok(())
}

fn encoded_variable_integer_len(value: usize) -> usize {
    match value {
        0x00..=0x07F => 1,
        0x80..=0x3FFF => 2,
        0x4000..=0x1FFFFF => 3,
        _ => 4
    }
}