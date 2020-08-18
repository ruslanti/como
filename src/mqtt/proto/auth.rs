use std::convert::TryInto;
use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut, BufMut};
use crate::mqtt::proto::types::{ControlPacket, QoS, Publish, Subscribe, SubOption, Retain, SubAck, MqttString, Auth};
use crate::mqtt::proto::property::{PropertiesBuilder, Property, PublishProperties, SubscribeProperties, SubAckProperties, AuthProperties};
use crate::mqtt::proto::decoder::{decode_utf8_string, decode_variable_integer};
use crate::mqtt::proto::encoder::encode_utf8_string;

pub fn decode_auth(reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
    end_of_stream!(reader.remaining() < 1, "auth reason conde");
    let reason_code = reader.get_u8().try_into()?;
    let properties_length = decode_variable_integer(reader)? as usize;
    let properties = decode_auth_properties(&mut reader.split_to(properties_length))?;
    Ok(Some(ControlPacket::Auth(Auth {
        reason_code,
        properties
    })))
}

pub fn decode_auth_properties(reader: &mut BytesMut) -> Result<AuthProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(reader)?;
        match id.try_into()? {
            Property::AuthenticationMethod => {
                if let Some(authentication_method) = decode_utf8_string(reader)? {
                    builder = builder.authentication_method(authentication_method)?
                } else {
                    return Err(anyhow!("missing authentication method"))
                }
            },
            Property::AuthenticationData => {
                unimplemented!()
            }
            Property::ReasonString => {
                builder = builder.response_topic(decode_utf8_string(reader)?)?;
            },
            Property::UserProperty => {
                let user_property = (decode_utf8_string(reader)?, decode_utf8_string(reader)?);
                if let (Some(key), Some(value)) = user_property {
                    builder = builder.user_properties((key, value));
                }
            }
            _ => return Err(anyhow!("unknown subscribe property: {:x}", id))
        }
    }
    Ok(builder.auth())
}
