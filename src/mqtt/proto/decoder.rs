use std::convert::TryInto;

use anyhow::{anyhow, ensure, Context, Result};
use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;
use tracing::{instrument, trace};

use crate::mqtt::proto::auth::decode_auth;
use crate::mqtt::proto::connack::decode_connack;
use crate::mqtt::proto::connect::decode_connect;
use crate::mqtt::proto::disconnect::decode_disconnect;
use crate::mqtt::proto::publish::decode_publish;
use crate::mqtt::proto::pubres::{decode_puback, decode_pubcomp, decode_pubrec, decode_pubrel};
use crate::mqtt::proto::subscribe::decode_subscribe;
use crate::mqtt::proto::types::{ControlPacket, MQTTCodec, MqttString, PacketPart, PacketType};
use crate::mqtt::proto::unsubscribe::decode_unsubscribe;

const MIN_FIXED_HEADER_LEN: usize = 2;

impl Decoder for MQTTCodec {
    type Item = ControlPacket;
    type Error = anyhow::Error;

    #[instrument(skip(self), err)]
    fn decode(&mut self, reader: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.part {
            PacketPart::FixedHeader => {
                // trace!("fixed header");
                if reader.len() < MIN_FIXED_HEADER_LEN {
                    // trace!(?self.part, "src buffer may not have entire fixed header");
                    return Ok(None);
                }
                let packet_type: PacketType = reader.get_u8().try_into()?;
                let remaining = decode_variable_integer(reader)? as usize;
                reader.reserve(remaining);

                self.part = PacketPart::VariableHeader {
                    remaining,
                    packet_type,
                };
                self.decode(reader)
            }
            PacketPart::VariableHeader {
                remaining,
                packet_type,
            } => {
                // trace!("variable header");
                if reader.len() < remaining {
                    trace!(?self.part, "src buffer does not have entire variable header and payload");
                    return Ok(None);
                }
                self.part = PacketPart::FixedHeader;
                let packet = reader.split_to(remaining).freeze();
                match packet_type {
                    PacketType::CONNECT => decode_connect(packet),
                    PacketType::CONNACK => decode_connack(packet),
                    PacketType::PUBLISH { dup, qos, retain } => {
                        decode_publish(dup, qos, retain, packet)
                    }
                    PacketType::PUBACK => decode_puback(packet),
                    PacketType::PUBREC => decode_pubrec(packet),
                    PacketType::PUBREL => decode_pubrel(packet),
                    PacketType::PUBCOMP => decode_pubcomp(packet),
                    PacketType::SUBSCRIBE => decode_subscribe(packet),
                    PacketType::SUBACK => unimplemented!(),
                    PacketType::UNSUBSCRIBE => decode_unsubscribe(packet),
                    PacketType::UNSUBACK => unimplemented!(),
                    PacketType::PINGREQ => Ok(Some(ControlPacket::PingReq)),
                    PacketType::PINGRESP => Ok(Some(ControlPacket::PingResp)),
                    PacketType::DISCONNECT => decode_disconnect(packet),
                    PacketType::AUTH => decode_auth(packet),
                }
            }
        }
    }
}

pub fn decode_variable_integer<T>(reader: &mut T) -> Result<u32>
where
    T: Buf,
{
    let mut multiplier = 1;
    let mut value = 0;
    loop {
        ensure!(reader.remaining() > 0, anyhow!("end of stream"));
        let encoded_byte: u8 = reader.get_u8();
        value += (encoded_byte & 0x7F) as u32 * multiplier;
        ensure!(
            multiplier <= (0x80 * 0x80 * 0x80),
            anyhow!("malformed variable integer: {}", value)
        );
        multiplier *= 0x80;
        if (encoded_byte & 0x80) == 0 {
            break;
        }
    }
    Ok(value)
}

pub fn decode_utf8_string(reader: &mut Bytes) -> Result<Option<MqttString>> {
    if reader.remaining() >= 2 {
        let len = reader.get_u16() as usize;
        if reader.remaining() >= len {
            if len > 0 {
                Ok(Some(reader.split_to(len)))
            } else {
                Ok(None)
            }
        } else {
            Err(anyhow!("end of stream")).context("decode_utf8_string")
        }
    } else {
        Err(anyhow!("end of stream")).context("decode_utf8_string")
    }
}
