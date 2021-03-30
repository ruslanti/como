use std::convert::{TryFrom, TryInto};

use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::Encoder;

use crate::v5::decoder::{decode_utf8_string, decode_variable_integer};
use crate::v5::error::MqttError;
use crate::v5::error::MqttError::EndOfStream;
use crate::v5::error::MqttError::UnacceptableProperty;
use crate::v5::property::{AuthProperties, PropertiesBuilder, Property};
use crate::v5::types::{Auth, MqttCodec};

impl TryFrom<Bytes> for Auth {
    type Error = MqttError;

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
    type Error = MqttError;

    fn try_from(mut reader: Bytes) -> Result<Self, Self::Error> {
        let mut builder = PropertiesBuilder::default();
        while reader.has_remaining() {
            let id = decode_variable_integer(&mut reader)?;
            let property = id.try_into()?;
            match property {
                Property::AuthenticationMethod => {
                    if let Some(authentication_method) = decode_utf8_string(&mut reader)? {
                        builder = builder.authentication_method(authentication_method)?
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
                _ => return Err(UnacceptableProperty(property)),
            }
        }
        Ok(builder.auth())
    }
}

impl Encoder<Auth> for MqttCodec {
    type Error = MqttError;

    fn encode(&mut self, _msg: Auth, _writer: &mut BytesMut) -> Result<(), Self::Error> {
        unimplemented!()
    }
}
