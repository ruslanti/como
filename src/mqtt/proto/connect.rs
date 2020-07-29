use std::convert::TryInto;
use anyhow::{anyhow, Result};
use bytes::{BytesMut, Buf, Bytes};
use crate::mqtt::proto::types::{ControlPacket, Will, QoS};
use crate::mqtt::proto::property::{PropertiesBuilder, ConnectProperties, Property};
use crate::mqtt::proto::decoder::{decode_utf8_string, decode_variable_integer};
use crate::mqtt::proto::will::decode_will_properties;

pub fn decode_connect(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    if Some(Bytes::from("MQTT")) != decode_utf8_string(reader)? {
        return Err(anyhow!("wrong protocol name"))
    }
    end_of_stream!(reader.remaining() < 4, "connect version");
    if 5 != reader.get_u8() {
        return Err(anyhow!("wrong protocol version"))
    }

    let flags = reader.get_u8();
    let clean_start_flag = ((flags & 0b00000010) >> 1) != 0;
    let will_flag = ((flags & 0b00000100) >> 2) != 0;
    let will_qos_flag: QoS = ((flags & 0b00011000) >> 3).try_into()?;
    let will_retain_flag = ((flags & 0b00100000) >> 5) != 0;
    let password_flag = ((flags & 0b01000000) >> 6) != 0;
    let username_flag = ((flags & 0b10000000) >> 7) != 0;

    let keep_alive = reader.get_u16();

    let properties_length = decode_variable_integer(reader)? as usize;
    let properties = decode_connect_properties(&mut reader.split_to(properties_length))?;

    let client_identifier = decode_utf8_string(reader)?;

    let will = if will_flag {
        let will_properties_length = decode_variable_integer(reader)? as usize;
        let properties = decode_will_properties(&mut reader.split_to(will_properties_length))?;
        let topic = decode_utf8_string(reader)?;
        let payload = Default::default();
        Some(Will{
            qos: will_qos_flag,
            retain: will_retain_flag,
            properties,
            topic,
            payload
        })
    } else {
        None
    };

    let username = if username_flag {
        decode_utf8_string(reader)?
    } else {
        None
    };

    let password = if password_flag {
        decode_utf8_string(reader)?
    } else {
        None
    };

    Ok(Some(ControlPacket::Connect{
        clean_start_flag,
        keep_alive,
        properties,
        client_identifier,
        username,
        password,
        will
    }))
}

fn decode_connect_properties(reader: &mut BytesMut) -> Result<ConnectProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(reader)?;
        match id.try_into()? {
            Property::SessionExpireInterval => {
                end_of_stream!(reader.remaining() < 4, "session expire interval");
                builder = builder.session_expire_interval(reader.get_u32())?;
            },
            Property::ReceiveMaximum => {
                end_of_stream!(reader.remaining() < 2, "receive maximum");
                builder = builder.receive_maximum(reader.get_u16())?;
            },
            Property::MaximumPacketSize => {
                end_of_stream!(reader.remaining() < 4, "maximum packet size");
                builder = builder.maximum_packet_size(reader.get_u32())?;
            },
            Property::TopicAliasMaximum => {
                end_of_stream!(reader.remaining() < 2, "topic alias maximum");
                builder = builder.topic_alias_maximum(reader.get_u16())?;
            }
            Property::RequestResponseInformation => {
                end_of_stream!(reader.remaining() < 1, "request response information");
                builder = builder.request_response_information(reader.get_u8())?;
            },
            Property::RequestProblemInformation => {
                end_of_stream!(reader.remaining() < 1, "request problem information");
                builder = builder.request_problem_information(reader.get_u8())?;
            },
            Property::UserProperty => {
                unimplemented!()
            }
            Property::AuthenticationMethod => {
                unimplemented!()
            }
            _ => return Err(anyhow!("unknown connect property: {:x}", id))
        }
    }
    Ok(builder.connect())
}
