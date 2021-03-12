use std::convert::{TryFrom, TryInto};

use anyhow::{anyhow, bail, ensure, Result};
use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Encoder;

use crate::v5::decoder::{decode_utf8_string, decode_variable_integer};
use crate::v5::property::{AuthProperties, PropertiesBuilder, Property};
use crate::v5::types::{Auth, MQTTCodec};

impl TryFrom<Bytes> for Auth {
    type Error = anyhow::Error;

    fn try_from(mut reader: Bytes) -> Result<Self, Self::Error> {
        end_of_stream!(reader.remaining() < 1, "auth reason code");
        let reason_code = reader.get_u8().try_into()?;
        let properties_length = decode_variable_integer(&mut reader)? as usize;
        let properties = AuthProperties::try_from(reader.slice(..properties_length))?;
        Ok(Auth {
            reason_code,
            properties,
        })
    }
}

impl TryFrom<Bytes> for AuthProperties {
    type Error = anyhow::Error;

    fn try_from(mut reader: Bytes) -> Result<Self, Self::Error> {
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
}

impl Encoder<Auth> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, _msg: Auth, _writer: &mut BytesMut) -> Result<(), Self::Error> {
        unimplemented!()
    }
}
