use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::Encoder;

use crate::v5::error::MqttError;
use crate::v5::error::MqttError::EndOfStream;
use crate::v5::property::PropertiesSize;
use crate::v5::string::MqttString;
use crate::v5::types::{ControlPacket, MqttCodec, PacketType};

pub trait RemainingLength {
    fn remaining_length(&self) -> usize;
}

impl Encoder<ControlPacket> for MqttCodec {
    type Error = MqttError;

    fn encode(&mut self, packet: ControlPacket, writer: &mut BytesMut) -> Result<(), Self::Error> {
        match packet {
            ControlPacket::Connect(connect) => {
                encode_fixed_header(writer, PacketType::Connect, connect.remaining_length())?;
                self.encode(connect, writer)?
            }
            ControlPacket::ConnAck(connack) => {
                encode_fixed_header(writer, PacketType::ConnAck, connack.remaining_length())?;
                self.encode(connack, writer)?
            }
            ControlPacket::Publish(publish) => {
                encode_fixed_header(
                    writer,
                    PacketType::Publish {
                        dup: publish.dup,
                        qos: publish.qos,
                        retain: publish.retain,
                    },
                    publish.remaining_length(),
                )?;
                self.encode(publish, writer)?
            }
            ControlPacket::PubAck(response) => {
                encode_fixed_header(writer, PacketType::PubAck, response.remaining_length())?;
                self.encode(response, writer)?
            }
            ControlPacket::PubRec(response) => {
                encode_fixed_header(writer, PacketType::PubRec, response.remaining_length())?;
                self.encode(response, writer)?
            }
            ControlPacket::PubRel(response) => {
                encode_fixed_header(writer, PacketType::PubRel, response.remaining_length())?;
                self.encode(response, writer)?
            }
            ControlPacket::PubComp(response) => {
                encode_fixed_header(writer, PacketType::PubComp, response.remaining_length())?;
                self.encode(response, writer)?
            }
            ControlPacket::Subscribe(subscribe) => {
                encode_fixed_header(writer, PacketType::Subscribe, subscribe.remaining_length())?;
                self.encode(subscribe, writer)?
            }
            ControlPacket::SubAck(response) => {
                encode_fixed_header(writer, PacketType::SubAck, response.remaining_length())?;
                self.encode(response, writer)?
            }
            ControlPacket::UnSubscribe(unsubscribe) => {
                encode_fixed_header(
                    writer,
                    PacketType::UnSubscribe,
                    unsubscribe.remaining_length(),
                )?;
                self.encode(unsubscribe, writer)?
            }
            ControlPacket::UnSubAck(response) => {
                encode_fixed_header(writer, PacketType::UnSubAck, response.remaining_length())?;
                self.encode(response, writer)?
            }
            ControlPacket::PingReq => {
                writer.reserve(2);
                writer.put_u8(PacketType::PingReq.into()); // packet type
                writer.put_u8(0); // remaining length
            }
            ControlPacket::PingResp => {
                writer.reserve(2);
                writer.put_u8(PacketType::PingResp.into()); // packet type
                writer.put_u8(0); // remaining length
            }
            ControlPacket::Disconnect(disconnect) => {
                encode_fixed_header(
                    writer,
                    PacketType::Disconnect,
                    disconnect.remaining_length(),
                )?;
                self.encode(disconnect, writer)?
            }
            ControlPacket::Auth(_auth) => unimplemented!(),
        };
        Ok(())
    }
}

impl Encoder<usize> for MqttCodec {
    type Error = MqttError;

    fn encode(&mut self, v: usize, writer: &mut BytesMut) -> Result<(), Self::Error> {
        let mut value = v;
        loop {
            let mut encoded_byte = (value % 0x80) as u8;
            value /= 0x80;
            // if there are more data to encode, set the top bit of this byte
            if value > 0 {
                encoded_byte |= 0x80;
            }
            end_of_stream!(writer.capacity() < 1, "encode variable integer");
            writer.put_u8(encoded_byte);
            if value == 0 {
                break;
            }
        }
        Ok(())
    }
}

impl Encoder<Bytes> for MqttCodec {
    type Error = MqttError;

    fn encode(&mut self, b: Bytes, writer: &mut BytesMut) -> Result<(), Self::Error> {
        end_of_stream!(writer.capacity() < b.len(), "encode bytes");
        writer.put(b);
        Ok(())
    }
}

fn encode_fixed_header(
    writer: &mut BytesMut,
    packet_type: PacketType,
    remaining_length: usize,
) -> Result<(), MqttError> {
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

pub fn encode_utf8_string(writer: &mut BytesMut, s: MqttString) -> Result<(), MqttError> {
    end_of_stream!(writer.capacity() < 2, "encode utf2 string len");
    writer.put_u16(s.len() as u16);
    end_of_stream!(writer.capacity() < s.len(), "encode utf2 string");
    writer.put(s);
    Ok(())
}

pub fn encode_variable_integer(writer: &mut BytesMut, v: usize) -> Result<(), MqttError> {
    let mut value = v;
    loop {
        let mut encoded_byte = (value % 0x80) as u8;
        value /= 0x80;
        // if there are more data to encode, set the top bit of this byte
        if value > 0 {
            encoded_byte |= 0x80;
        }
        end_of_stream!(writer.capacity() < 1, "encode variable integer");
        writer.put_u8(encoded_byte);
        if value == 0 {
            break;
        }
    }
    Ok(())
}
