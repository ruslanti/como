use std::convert::TryFrom;

use anyhow::{anyhow, ensure, Result};
use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Decoder;
use tracing::{instrument, trace};

use crate::v5::connack::decode_connack;
use crate::v5::disconnect::decode_disconnect;
use crate::v5::publish::decode_publish;
use crate::v5::string::MqttString;
use crate::v5::subscribe::decode_subscribe;
use crate::v5::types::{
    Auth, Connect, ControlPacket, MQTTCodec, PacketPart, PacketType, PublishResponse, SubAck,
};
use crate::v5::unsubscribe::decode_unsubscribe;

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
                let packet_type = PacketType::try_from(reader.get_u8())?;
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
                    PacketType::CONNECT => Connect::try_from(packet)
                        .map(|connect| Some(ControlPacket::Connect(connect))),
                    PacketType::CONNACK => decode_connack(packet),
                    PacketType::PUBLISH { dup, qos, retain } => {
                        decode_publish(dup, qos, retain, packet)
                    }
                    PacketType::PUBACK => PublishResponse::try_from(packet)
                        .map(|response| Some(ControlPacket::PubAck(response))),
                    PacketType::PUBREC => PublishResponse::try_from(packet)
                        .map(|response| Some(ControlPacket::PubRec(response))),
                    PacketType::PUBREL => PublishResponse::try_from(packet)
                        .map(|response| Some(ControlPacket::PubRel(response))),
                    PacketType::PUBCOMP => PublishResponse::try_from(packet)
                        .map(|response| Some(ControlPacket::PubComp(response))),
                    PacketType::SUBSCRIBE => decode_subscribe(packet),
                    PacketType::SUBACK => {
                        SubAck::try_from(packet).map(|s| Some(ControlPacket::SubAck(s)))
                    }
                    PacketType::UNSUBSCRIBE => decode_unsubscribe(packet),
                    PacketType::UNSUBACK => unimplemented!(),
                    PacketType::PINGREQ => Ok(Some(ControlPacket::PingReq)),
                    PacketType::PINGRESP => Ok(Some(ControlPacket::PingResp)),
                    PacketType::DISCONNECT => decode_disconnect(packet),
                    PacketType::AUTH => {
                        Auth::try_from(packet).map(|a| Some(ControlPacket::Auth(a)))
                    }
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
                Ok(Some(MqttString::from(reader.split_to(len))))
            } else {
                Ok(None)
            }
        } else {
            Err(anyhow!("end of stream"))
        }
    } else {
        Err(anyhow!("end of stream"))
    }
}
