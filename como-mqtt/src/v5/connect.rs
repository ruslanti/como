use std::convert::TryInto;
use std::mem::size_of_val;

use anyhow::{anyhow, bail, ensure, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::Encoder;

use crate::v5::decoder::{decode_utf8_string, decode_variable_integer};
use crate::v5::encoder::{encode_utf8_string, RemainingLength};
use crate::v5::property::{ConnectProperties, PropertiesBuilder, PropertiesSize, Property};
use crate::v5::types::{Connect, ControlPacket, MQTTCodec, Will, MQTT, VERSION};
use crate::v5::will::decode_will_properties;

pub fn decode_connect(mut reader: Bytes) -> Result<Option<ControlPacket>> {
    if Some(Bytes::from(MQTT)) != decode_utf8_string(&mut reader)? {
        bail!("wrong protocol name");
    }
    end_of_stream!(reader.remaining() < 4, "connect version");
    if VERSION != reader.get_u8() {
        bail!("wrong protocol version");
    }

    let flags = reader.get_u8();
    let (
        clean_start_flag,
        will_flag,
        will_qos_flag,
        will_retain_flag,
        _password_flag,
        _username_flag,
    ) = Connect::set_flags(flags)?;

    let keep_alive = reader.get_u16();

    let properties_length = decode_variable_integer(&mut reader)
        .context("connect properties length decode error")? as usize;
    let properties = decode_connect_properties(reader.split_to(properties_length))
        .context("connect properties decode error")?;

    let client_identifier =
        decode_utf8_string(&mut reader).context("client_identifier decode error")?;

    let will = if will_flag {
        let will_properties_length = decode_variable_integer(&mut reader)
            .context("will properties length decode error")?
            as usize;
        let properties = decode_will_properties(reader.split_to(will_properties_length))
            .context("will properties decode error")?;
        let topic = decode_utf8_string(&mut reader)
            .context("will topic decode error")?
            .ok_or(anyhow!("will topic is missing"))?;
        let payload = reader;
        Some(Will {
            qos: will_qos_flag,
            retain: will_retain_flag,
            properties,
            topic,
            payload,
        })
    } else {
        None
    };

    /*
        TODO: get username and password
        let username = if username_flag {
            decode_utf8_string(&mut reader)?
        } else {
            None
        };

        let password = if password_flag {
            decode_utf8_string(&mut reader)?
        } else {
            None
        };
    */
    Ok(Some(ControlPacket::Connect(Connect {
        clean_start_flag,
        keep_alive,
        properties,
        client_identifier,
        username: None,
        password: None,
        will,
    })))
}

fn decode_connect_properties(mut reader: Bytes) -> Result<ConnectProperties> {
    let mut builder = PropertiesBuilder::new();
    while reader.has_remaining() {
        let id = decode_variable_integer(&mut reader)?;
        match id.try_into()? {
            Property::SessionExpireInterval => {
                end_of_stream!(reader.remaining() < 4, "session expire interval");
                builder = builder.session_expire_interval(reader.get_u32())?;
            }
            Property::ReceiveMaximum => {
                end_of_stream!(reader.remaining() < 2, "receive maximum");
                builder = builder.receive_maximum(reader.get_u16())?;
            }
            Property::MaximumPacketSize => {
                end_of_stream!(reader.remaining() < 4, "maximum packet size");
                builder = builder.maximum_packet_size(reader.get_u32())?;
            }
            Property::TopicAliasMaximum => {
                end_of_stream!(reader.remaining() < 2, "topic alias maximum");
                builder = builder.topic_alias_maximum(reader.get_u16())?;
            }
            Property::RequestResponseInformation => {
                end_of_stream!(reader.remaining() < 1, "request response information");
                builder = builder.request_response_information(reader.get_u8())?;
            }
            Property::RequestProblemInformation => {
                end_of_stream!(reader.remaining() < 1, "request problem information");
                builder = builder.request_problem_information(reader.get_u8())?;
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
            Property::AuthenticationMethod => {
                if let Some(authentication_method) = decode_utf8_string(&mut reader)? {
                    builder = builder.authentication_method(authentication_method)?
                } else {
                    bail!("missing authentication method");
                }
            }
            _ => bail!("unknown connect property: {:x}", id),
        }
    }
    Ok(builder.connect())
}

impl Encoder<ConnectProperties> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        properties: ConnectProperties,
        writer: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.encode(properties.size(), writer)?;
        encode_property_u32!(
            writer,
            SessionExpireInterval,
            properties.session_expire_interval
        );
        encode_property_u16!(writer, ReceiveMaximum, properties.receive_maximum);
        encode_property_u32!(writer, MaximumPacketSize, properties.maximum_packet_size);
        encode_property_u16!(writer, TopicAliasMaximum, properties.topic_alias_maximum);
        encode_property_u8!(
            writer,
            RequestResponseInformation,
            properties.request_response_information.map(|b| b as u8)
        );
        encode_property_u8!(
            writer,
            RequestProblemInformation,
            properties.request_problem_information.map(|b| b as u8)
        );
        encode_property_user_properties!(writer, UserProperty, properties.user_properties);
        encode_property_string!(
            writer,
            AuthenticationMethod,
            properties.authentication_method
        );
        Ok(())
    }
}

impl RemainingLength for Connect {
    fn remaining_length(&self) -> usize {
        let properties_len = self.properties.size();
        10 + properties_len.size()
            + properties_len
            + self
                .client_identifier
                .as_ref()
                .map(|s| 2 + s.len())
                .unwrap_or(2)
            + self.will.as_ref().map(|w| w.size()).unwrap_or(0)
            + self.username.as_ref().map(|u| 2 + u.len()).unwrap_or(0)
            + self.password.as_ref().map(|p| 2 + p.len()).unwrap_or(0)
    }
}

impl Encoder<Connect> for MQTTCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, msg: Connect, writer: &mut BytesMut) -> Result<(), Self::Error> {
        self.encode(Bytes::from_static(MQTT.as_ref()), writer)?;
        writer.put_u8(VERSION);
        writer.put_u8(msg.get_flags());
        writer.put_u16(msg.keep_alive);
        self.encode(msg.properties, writer)?;
        self.encode(msg.client_identifier.unwrap_or(Bytes::new()), writer)?;
        if let Some(will) = msg.will {
            self.encode(will, writer)?;
        }

        Ok(())
    }
}

impl PropertiesSize for ConnectProperties {
    fn size(&self) -> usize {
        let mut len = check_size_of!(self, session_expire_interval);
        len += check_size_of!(self, receive_maximum);
        len += check_size_of!(self, maximum_packet_size);
        len += check_size_of!(self, topic_alias_maximum);
        len += check_size_of!(self, request_response_information);
        len += check_size_of!(self, request_problem_information);
        len += self
            .user_properties
            .iter()
            .map(|(x, y)| 5 + x.len() + y.len())
            .sum::<usize>();
        len += check_size_of_string!(self, authentication_method);
        len
    }
}
