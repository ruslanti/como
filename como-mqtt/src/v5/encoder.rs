use anyhow::{anyhow, ensure, Result};
use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::Encoder;

use crate::v5::property::PropertiesSize;
use crate::v5::types::{ControlPacket, MQTTCodec, MqttString, PacketType};

pub trait RemainingLength {
    fn remaining_length(&self) -> usize;
}

impl Encoder<ControlPacket> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, packet: ControlPacket, writer: &mut BytesMut) -> Result<(), Self::Error> {
        match packet {
            ControlPacket::Connect(connect) => {
                encode_fixed_header(writer, PacketType::CONNECT, connect.remaining_length())?;
                self.encode(connect, writer)?
            }
            ControlPacket::ConnAck(connack) => {
                encode_fixed_header(writer, PacketType::CONNACK, connack.remaining_length())?;
                self.encode(connack, writer)?
            }
            ControlPacket::Publish(publish) => {
                encode_fixed_header(
                    writer,
                    PacketType::PUBLISH {
                        dup: publish.dup,
                        qos: publish.qos,
                        retain: publish.retain,
                    },
                    publish.remaining_length(),
                )?;
                self.encode(publish, writer)?
            }
            ControlPacket::PubAck(response) => {
                encode_fixed_header(writer, PacketType::PUBACK, response.remaining_length())?;
                self.encode(response, writer)?
            }
            ControlPacket::PubRec(response) => {
                encode_fixed_header(writer, PacketType::PUBREC, response.remaining_length())?;
                self.encode(response, writer)?
            }
            ControlPacket::PubRel(response) => {
                encode_fixed_header(writer, PacketType::PUBREL, response.remaining_length())?;
                self.encode(response, writer)?
            }
            ControlPacket::PubComp(response) => {
                encode_fixed_header(writer, PacketType::PUBCOMP, response.remaining_length())?;
                self.encode(response, writer)?
            }
            ControlPacket::Subscribe(subscribe) => {
                encode_fixed_header(writer, PacketType::SUBSCRIBE, subscribe.remaining_length())?;
                self.encode(subscribe, writer)?
            }
            ControlPacket::SubAck(response) => {
                encode_fixed_header(writer, PacketType::SUBACK, response.remaining_length())?;
                self.encode(response, writer)?
            }
            ControlPacket::UnSubscribe(unsubscribe) => {
                encode_fixed_header(
                    writer,
                    PacketType::UNSUBSCRIBE,
                    unsubscribe.remaining_length(),
                )?;
                self.encode(unsubscribe, writer)?
            }
            ControlPacket::UnSubAck(response) => {
                encode_fixed_header(writer, PacketType::UNSUBACK, response.remaining_length())?;
                self.encode(response, writer)?
            }
            ControlPacket::PingReq => {
                writer.reserve(2);
                writer.put_u8(PacketType::PINGREQ.into()); // packet type
                writer.put_u8(0); // remaining length
            }
            ControlPacket::PingResp => {
                writer.reserve(2);
                writer.put_u8(PacketType::PINGRESP.into()); // packet type
                writer.put_u8(0); // remaining length
            }
            ControlPacket::Disconnect(disconnect) => {
                encode_fixed_header(
                    writer,
                    PacketType::DISCONNECT,
                    disconnect.remaining_length(),
                )?;
                self.encode(disconnect, writer)?
            }
            ControlPacket::Auth(_auth) => unimplemented!(),
        };
        Ok(())
    }
}

impl Encoder<usize> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, v: usize, writer: &mut BytesMut) -> Result<(), Self::Error> {
        let mut value = v;
        loop {
            let mut encoded_byte = (value % 0x80) as u8;
            value /= 0x80;
            // if there are more data to encode, set the top bit of this byte
            if value > 0 {
                encoded_byte |= 0x80;
            }
            ensure!(writer.capacity() > 0, anyhow!("end of stream"));
            writer.put_u8(encoded_byte);
            if value == 0 {
                break;
            }
        }
        Ok(())
    }
}

impl Encoder<MqttString> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, s: MqttString, writer: &mut BytesMut) -> Result<(), Self::Error> {
        end_of_stream!(writer.capacity() < 2, "encode utf2 string len");
        writer.put_u16(s.len() as u16);
        end_of_stream!(writer.capacity() < s.len(), "encode utf2 string");
        writer.put(s);
        Ok(())
    }
}

fn encode_fixed_header(
    writer: &mut BytesMut,
    packet_type: PacketType,
    remaining_length: usize,
) -> Result<()> {
    writer.reserve(1 + remaining_length.size() + remaining_length);
    writer.put_u8(packet_type.into()); // packet type
    encode_variable_integer(writer, remaining_length) // remaining length
}

impl PropertiesSize for usize {
    fn size(&self) -> usize {
        match self {
            0x00..=0x07F => 1,
            0x80..=0x3FFF => 2,
            0x4000..=0x1FFFFF => 3,
            _ => 4,
        }
    }
}

pub fn encode_utf8_string(writer: &mut BytesMut, s: Bytes) -> Result<()> {
    end_of_stream!(writer.capacity() < 2, "encode utf2 string len");
    writer.put_u16(s.len() as u16);
    end_of_stream!(writer.capacity() < s.len(), "encode utf2 string");
    writer.put(s);
    Ok(())
}

pub fn encode_variable_integer(writer: &mut BytesMut, v: usize) -> Result<()> {
    let mut value = v;
    loop {
        let mut encoded_byte = (value % 0x80) as u8;
        value /= 0x80;
        // if there are more data to encode, set the top bit of this byte
        if value > 0 {
            encoded_byte |= 0x80;
        }
        ensure!(writer.capacity() > 0, anyhow!("end of stream"));
        writer.put_u8(encoded_byte);
        if value == 0 {
            break;
        }
    }
    Ok(())
}
