use std::convert::TryInto;

use anyhow::{anyhow, bail, ensure, Result};
use bytes::{Buf, Bytes};

use crate::mqtt::proto::decoder::{decode_utf8_string, decode_variable_integer};
use crate::mqtt::proto::property::{AuthProperties, PropertiesBuilder, Property};
use crate::mqtt::proto::types::{Auth, ControlPacket};

pub fn decode_auth(mut reader: Bytes) -> Result<Option<ControlPacket>> {
    end_of_stream!(reader.remaining() < 1, "auth reason conde");
    let reason_code = reader.get_u8().try_into()?;
    let properties_length = decode_variable_integer(&mut reader)? as usize;
    let properties = decode_auth_properties(reader.slice(0..properties_length))?;
    Ok(Some(ControlPacket::Auth(Auth {
        reason_code,
        properties,
    })))
}

pub fn decode_auth_properties(mut reader: Bytes) -> Result<AuthProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(&mut reader)?;
        match id.try_into()? {
            Property::AuthenticationMethod => {
                if let Some(authentication_method) = decode_utf8_string(&mut reader)? {
                    builder = builder.authentication_method(authentication_method)?
                } else {
                    bail!("missing authentication method");
                }
            }
            Property::AuthenticationData => unimplemented!(),
            Property::ReasonString => {
                builder = builder.response_topic(decode_utf8_string(&mut reader)?)?;
            }
            Property::UserProperty => {
                let user_property = (
                    decode_utf8_string(&mut reader)?,
                    decode_utf8_string(&mut reader)?,
                );
                if let (Some(key), Some(value)) = user_property {
                    builder = builder.user_properties((key, value));
                }
            }
            _ => bail!("unknown subscribe property: {:x}", id),
        }
    }
    Ok(builder.auth())
}
