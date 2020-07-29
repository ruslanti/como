use std::convert::TryInto;
use bytes::{BytesMut, Buf, Bytes};
use tokio_util::codec::Decoder;
use tracing::{trace, instrument};
use anyhow::{anyhow, Result, Context};
use crate::mqtt::proto::types::{MQTTCodec, ControlPacket, PacketPart, PacketType};
use crate::mqtt::proto::connect::decode_connect;
use crate::mqtt::proto::connack::decode_connack;
use crate::mqtt::proto::publish::decode_publish;
use crate::mqtt::proto::disconnect::decode_disconnect;
use crate::mqtt::proto::pubres::{decode_puback, decode_pubrec, decode_pubrel};

const MIN_FIXED_HEADER_LEN: usize = 2;

impl Decoder for MQTTCodec {
    type Item = ControlPacket;
    type Error = anyhow::Error;

    #[instrument]
    fn decode(&mut self, reader: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.part {
            PacketPart::FixedHeader => {
                trace!("fixed header");
                if reader.len() < MIN_FIXED_HEADER_LEN {
                   // trace!(?self.part, "src buffer may not have entire fixed header");
                    return Ok(None)
                }
                let packet_type: PacketType = reader.get_u8().try_into()?;
                let remaining = decode_variable_integer(reader)? as usize;
                reader.reserve(remaining);

                self.part = PacketPart::VariableHeader {
                    remaining,
                    packet_type
                };
                self.decode(reader)
            },
            PacketPart::VariableHeader{remaining, packet_type} => {
                trace!("variable header");
                if reader.len() < remaining {
                    trace!(?self.part, "src buffer does not have entire variable header and payload");
                    return Ok(None)
                }
                self.part = PacketPart::FixedHeader;
                let mut packet = reader.split_to(remaining);
                match packet_type {
                    PacketType::CONNECT     => decode_connect(&mut packet),
                    PacketType::CONNACK     => decode_connack(&mut packet),
                    PacketType::PUBLISH{dup, qos, retain} => decode_publish(dup, qos, retain, &mut packet),
                    PacketType::PUBACK      => decode_puback(&mut packet),
                    PacketType::PUBREC      => decode_pubrec(&mut packet),
                    PacketType::PUBREL      => decode_pubrel(&mut packet),
                    PacketType::PUBCOMP     => unimplemented!(),
                    PacketType::SUBSCRIBE   => unimplemented!(),
                    PacketType::SUBACK      => unimplemented!(),
                    PacketType::UNSUBSCRIBE => unimplemented!(),
                    PacketType::UNSUBACK    => unimplemented!(),
                    PacketType::PINGREQ     => unimplemented!(),
                    PacketType::PINGRESP    => unimplemented!(),
                    PacketType::DISCONNECT  => decode_disconnect(&mut packet),
                    PacketType::AUTH        => unimplemented!(),
                }
            }
        }
    }
}

pub fn decode_variable_integer(reader: &mut BytesMut) -> Result<u32> {
    let mut multiplier = 1;
    let mut value = 0;
    loop {
        if reader.remaining() < 1 { return Err(anyhow!("end of stream")).context("decode_variable_integer") }
        let encoded_byte: u8 = reader.get_u8().into();
        value += (encoded_byte & 0x7F) as u32 * multiplier;
        if multiplier > (0x80 * 0x80 * 0x80) {
            return Err(anyhow!("malformed variable integer: {}", value)).context("decode_variable_integer")
        }
        multiplier *= 0x80;
        if (encoded_byte & 0x80) == 0 {
            break;
        }
    }
    Ok(value)
}

pub fn decode_utf8_string(reader: &mut BytesMut) -> Result<Option<Bytes>> {
    if reader.remaining() >= 2 {
        let len = reader.get_u16() as usize;
        if reader.remaining() >= len {
            if len > 0 { Ok(Some(reader.split_to(len).to_bytes())) } else { Ok(None) }
        } else { Err(anyhow!("end of stream")).context("decode_utf8_string") }
    } else { Err(anyhow!("end of stream")).context("decode_utf8_string") }
}