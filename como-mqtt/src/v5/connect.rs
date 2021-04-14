use std::convert::{TryFrom, TryInto};
use std::mem::size_of_val;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::Encoder;

use crate::v5::decoder::{decode_utf8_string, decode_variable_integer};
use crate::v5::encoder::{encode_utf8_string, RemainingLength};
use crate::v5::error::MqttError;
use crate::v5::error::MqttError::{EndOfStream, MalformedPacket, UnacceptableProperty};
use crate::v5::error::MqttError::{UnspecifiedError, UnsupportedProtocolVersion};
use crate::v5::property::{
    ConnectProperties, PropertiesBuilder, PropertiesSize, Property, WillProperties,
};
use crate::v5::string::MqttString;
use crate::v5::types::{Connect, MqttCodec, Will, MQTT, VERSION};

impl TryFrom<Bytes> for Connect {
    type Error = MqttError;

    fn try_from(mut reader: Bytes) -> Result<Self, Self::Error> {
        if let Some(protocol) = decode_utf8_string(&mut reader)? {
            let protocol = protocol.try_into()?;
            if MQTT != protocol {
                return Err(UnsupportedProtocolVersion(protocol));
            }
        } else {
            return Err(UnsupportedProtocolVersion(String::new()));
        }
        end_of_stream!(reader.remaining() < 4, "connect version");
        let version = reader.get_u8();
        if VERSION != version {
            return Err(UnsupportedProtocolVersion(version.to_string()));
        }

        let flags = reader.get_u8();
        let (
            reserved,
            clean_start_flag,
            will_flag,
            will_qos_flag,
            will_retain_flag,
            password_flag,
            username_flag,
        ) = Connect::set_flags(flags)?;
        if reserved {
            return Err(MalformedPacket);
        }

        let keep_alive = reader.get_u16();

        let properties_length = decode_variable_integer(&mut reader)? as usize;
        let properties = ConnectProperties::try_from(reader.split_to(properties_length))?;

        let client_identifier = decode_utf8_string(&mut reader)?;

        let will = if will_flag {
            let will_properties_length = decode_variable_integer(&mut reader)? as usize;
            let properties = WillProperties::try_from(reader.split_to(will_properties_length))?;
            let topic = decode_utf8_string(&mut reader)?
                .ok_or_else(|| UnspecifiedError("will topic is missing".to_owned()))?;
            let payload_len = reader.get_u16() as usize;
            let payload = reader.split_to(payload_len);

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

        let username = if username_flag {
            decode_utf8_string(&mut reader)?
        } else {
            None
        };

        let password = if password_flag {
            let len = reader.get_u16() as usize;
            Some(reader.split_to(len))
        } else {
            None
        };

        Ok(Connect {
            reserved,
            clean_start_flag,
            keep_alive,
            properties,
            client_identifier,
            username,
            password,
            will,
        })
    }
}

impl TryFrom<Bytes> for ConnectProperties {
    type Error = MqttError;

    fn try_from(mut reader: Bytes) -> Result<Self, Self::Error> {
        let mut builder = PropertiesBuilder::default();
        while reader.has_remaining() {
            let id = decode_variable_integer(&mut reader)?;
            let property = id.try_into()?;
            match property {
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
                    }
                }
                _ => return Err(UnacceptableProperty(property)),
            }
        }
        Ok(builder.connect())
    }
}

impl Encoder<ConnectProperties> for MqttCodec {
    type Error = MqttError;

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

impl Encoder<Connect> for MqttCodec {
    type Error = MqttError;

    fn encode(&mut self, msg: Connect, writer: &mut BytesMut) -> Result<(), Self::Error> {
        self.encode(MqttString::from(MQTT), writer)?;
        writer.put_u8(VERSION);
        writer.put_u8(msg.get_flags());
        writer.put_u16(msg.keep_alive);
        self.encode(msg.properties, writer)?;
        self.encode(msg.client_identifier.unwrap_or_default(), writer)?;
        if let Some(will) = msg.will {
            self.encode(will, writer)?;
        }
        if let Some(username) = msg.username {
            self.encode(username, writer)?;
        }
        if let Some(password) = msg.password {
            writer.put_u16(password.len() as u16);
            self.encode(password, writer)?;
        }

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
