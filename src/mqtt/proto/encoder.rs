use crate::mqtt::proto::types::{MQTTCodec, ControlPacket, PacketType, Connect, ConnAck, Publish, PubResp, Disconnect};
use tokio_util::codec::Encoder;
use bytes::{BytesMut, BufMut, Bytes};
use anyhow::{anyhow, Result, Context};
use crate::mqtt::proto::property::{PropertiesLength};
use crate::mqtt::proto::pubres::encode_pubres_properties;
use crate::mqtt::proto::disconnect::encode_disconnect_properties;

impl Encoder<Connect> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, _msg: Connect, _writer: &mut BytesMut) -> Result<(), Self::Error> {
        unimplemented!()
    }
}

impl Encoder<ConnAck> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, msg: ConnAck, writer: &mut BytesMut) -> Result<(), Self::Error> {
        let properties_length = msg.properties.len(); // properties
        let properties_length_size= encoded_variable_integer_len(properties_length); // properties length
        let remaining_length = 2 + properties_length_size + properties_length;
        let remaining_length_size = encoded_variable_integer_len(remaining_length); // remaining length size
        writer.reserve(1 + remaining_length_size + remaining_length);

        writer.put_u8(PacketType::CONNACK.into()); // packet type
        encode_variable_integer(writer, remaining_length)?; // remaining length
        writer.put_u8(msg.session_present as u8); // connack flags
        writer.put_u8(msg.reason_code.into());
        encode_variable_integer(writer, properties_length)?; // properties length
        self.encode(msg.properties, writer)
    }
}

impl Encoder<Publish> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, _msg: Publish, _writer: &mut BytesMut) -> Result<(), Self::Error> {
        unimplemented!()
    }
}

impl Encoder<PubResp> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, msg: PubResp, writer: &mut BytesMut) -> Result<(), Self::Error> {
        let properties_length = msg.properties.len(); // properties
        let properties_length_size= encoded_variable_integer_len(properties_length); // properties length
        let remaining_length = 3 + properties_length_size + properties_length;
        let remaining_length_size = encoded_variable_integer_len(remaining_length); // remaining length size
        writer.reserve(1 + remaining_length_size + remaining_length);

        writer.put_u8(msg.packet_type.into()); // packet type
        encode_variable_integer(writer, remaining_length)?; // remaining length
        writer.put_u16(msg.packet_identifier); // packet identifier
        writer.put_u8(msg.reason_code.into());
        encode_variable_integer(writer, properties_length)?; // properties length
        encode_pubres_properties(writer, msg.properties)
    }
}

impl Encoder<Disconnect> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, msg: Disconnect, writer: &mut BytesMut) -> Result<(), Self::Error> {
        let properties_length = msg.properties.len(); // properties
        let properties_length_size= encoded_variable_integer_len(properties_length); // properties length
        let remaining_length = 3 + properties_length_size + properties_length;
        let remaining_length_size = encoded_variable_integer_len(remaining_length); // remaining length size
        writer.reserve(1 + remaining_length_size + remaining_length);

        writer.put_u8(PacketType::DISCONNECT.into()); // packet type
        encode_variable_integer(writer, remaining_length)?; // remaining length
        writer.put_u8(msg.reason_code.into());
        encode_variable_integer(writer, properties_length)?; // properties length
        encode_disconnect_properties(writer, msg.properties)
    }
}

impl Encoder<ControlPacket> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, packet: ControlPacket, writer: &mut BytesMut) -> Result<(), Self::Error> {
        match packet {
            ControlPacket::Connect(connect) => self.encode(connect, writer)?,
            ControlPacket::ConnAck(connack) => self.encode(connack, writer)?,
            ControlPacket::Publish(publish) => self.encode(publish, writer)?,
            ControlPacket::PubAck(response) => self.encode(response, writer)?,
            ControlPacket::PubRec(response) => self.encode(response, writer)?,
            ControlPacket::PubRel(response) => self.encode(response, writer)?,
            ControlPacket::PubComp(response) => self.encode(response, writer)?,
            ControlPacket::Subscribe => unimplemented!(),
            ControlPacket::SubAck => unimplemented!(),
            ControlPacket::UnSubscribe => unimplemented!(),
            ControlPacket::UnSubAck => unimplemented!(),
            ControlPacket::PingReq => {
                writer.reserve(2);
                writer.put_u8(PacketType::PINGREQ.into()); // packet type
                writer.put_u8(0); // remaining length
            },
            ControlPacket::PingResp => {
                writer.reserve(2);
                writer.put_u8(PacketType::PINGRESP.into()); // packet type
                writer.put_u8(0); // remaining length
            },
            ControlPacket::Disconnect(disconnect) => self.encode(disconnect, writer)?,
            ControlPacket::Auth => unimplemented!(),
        };
        Ok(())
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
        value = value / 0x80;
        // if there are more data to encode, set the top bit of this byte
        if value > 0 {
            encoded_byte = encoded_byte | 0x80;
        }
        if writer.capacity() < 1 { return Err(anyhow!("end of stream")).context("encode_variable_integer") }
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