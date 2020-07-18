use std::str;
use std::io::{Read, Cursor};
use std::convert::TryInto;
use bytes::{BytesMut, Buf, Bytes};
use byteorder::{ ReadBytesExt, BigEndian};
use tokio_util::codec::Decoder;
use tokio::io::{Error, AsyncBufRead, AsyncBufReadExt};
use tracing::{trace, debug, error, info, instrument};
use anyhow::{anyhow, Result, Context};
use bytes::buf::BufExt;
use crate::mqtt::proto::types::{MQTTCodec, ControlPacket, PacketPart, PacketType, QoS, Will};
use crate::mqtt::proto::property::{Property, ConnectProperties, WillProperties, ConnAckProperties, PropertiesBuilder};

const MAX_FIXED_HEADER_LEN: usize = 5;

impl Decoder for MQTTCodec {
    type Item = ControlPacket;
    type Error = anyhow::Error;

    #[instrument]
    fn decode(&mut self, reader: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.part {
            PacketPart::FixedHeader => {
                trace!("fixed header");
                if reader.len() < MAX_FIXED_HEADER_LEN {
                    trace!(?self.part, "src buffer may not have entire fixed header");
                    return Ok(None)
                }
                let packet_type: PacketType = reader.get_u8().try_into()?;
                let remaining = decode_variable_integer(reader)? as usize;
                reader.reserve(remaining);

                self.part = PacketPart::VariableHeader {
                    remaining,
                    packet_type
                };
                self.decode(reader)
            },
            PacketPart::VariableHeader{remaining, packet_type} => {
                trace!("variable header");
                if reader.len() < remaining {
                    trace!(?self.part, "src buffer does not have entire variable header and payload");
                    return Ok(None)
                }
                self.part = PacketPart::FixedHeader;
                let mut packet = reader.split_to(remaining);
                match packet_type {
                    PacketType::CONNECT     => self.decode_connect(&mut packet),
                    PacketType::CONNACK     => self.decode_connack(&mut packet),
                    PacketType::PUBLISH{dup, qos, retain} => Ok(None),
                    PacketType::PUBACK      => Ok(None),
                    PacketType::PUBREC      => Ok(None),
                    PacketType::PUBREL      => Ok(None),
                    PacketType::PUBCOMP     => Ok(None),
                    PacketType::SUBSCRIBE   => Ok(None),
                    PacketType::SUBACK      => Ok(None),
                    PacketType::UNSUBSCRIBE => Ok(None),
                    PacketType::UNSUBACK    => Ok(None),
                    PacketType::PINGREQ     => Ok(None),
                    PacketType::PINGRESP    => Ok(None),
                    PacketType::DISCONNECT  => Ok(None),
                    PacketType::AUTH        => Ok(None)
                }
            },
            PacketPart::Payload => {
                Ok(None)
            }
        }
    }
}

impl MQTTCodec {
    fn decode_connect(&self, reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
        if "MQTT" != decode_utf8_string(reader)? {
            return Err(anyhow!("wrong protocol name"))
        }
        end_of_stream!(reader.remaining() < 4);
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
            Some(decode_utf8_string(reader)?)
        } else {
            None
        };

        let password = if password_flag {
            Some(decode_utf8_string(reader)?)
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

    fn decode_connack(&self, reader: &mut BytesMut) -> Result<Option<ControlPacket>> {
        end_of_stream!(reader.remaining() < 3);
        let flags = reader.get_u8();
        let session_present = (flags & 0b00000001) != 0;
        let reason_code = reader.get_u8().try_into()?;
        let properties_length = decode_variable_integer(reader)? as usize;
        let properties = decode_connack_properties(&mut reader.split_to(properties_length))?;
        Ok(Some(ControlPacket::ConnAck {
            session_present,
            reason_code,
            properties
        }))
    }
}

fn decode_will_properties(reader: &mut BytesMut) -> Result<WillProperties> {
    let mut builder = PropertiesBuilder::new();
    while (reader.remaining() > 0) {
        let id = decode_variable_integer(reader)?;
        match id.try_into()? {
            Property::WillDelayInterval => {
                end_of_stream!(reader.remaining() < 4);
                builder = builder.will_delay_interval(reader.get_u32())?;
            },
            Property::PayloadFormatIndicator => {
                end_of_stream!(reader.remaining() < 1);
                builder = builder.payload_format_indicator(reader.get_u8())?;
            },
            Property::MessageExpireInterval => {
                end_of_stream!(reader.remaining() < 4);
                builder = builder.message_expire_interval(reader.get_u32())?;
            },
            Property::ContentType => {
                builder = builder.content_type(decode_utf8_string(reader)?)?;
            }
            Property::ResponseTopic => {
                builder = builder.response_topic(decode_utf8_string(reader)?)?;
            },
            Property::CorrelationData => {
                unimplemented!()
            },
            Property::UserProperty => {
                unimplemented!()
            }
            _ => return Err(anyhow!("unknown will property: {:x}", id))
        }
    }
    Ok(builder.will())
}

fn decode_connect_properties(reader: &mut BytesMut) -> Result<ConnectProperties> {
    let mut builder = PropertiesBuilder::new();
    while (reader.remaining() > 0) {
        let id = decode_variable_integer(reader)?;
        match id.try_into()? {
            Property::SessionExpireInterval => {
                end_of_stream!(reader.remaining() < 4);
                builder = builder.session_expire_interval(reader.get_u32())?;
            },
            Property::ReceiveMaximum => {
                end_of_stream!(reader.remaining() < 2);
                builder = builder.receive_maximum(reader.get_u16())?;
            },
            Property::MaximumPacketSize => {
                end_of_stream!(reader.remaining() < 4);
                builder = builder.maximum_packet_size(reader.get_u32())?;
            },
            Property::TopicAliasMaximum => {
                end_of_stream!(reader.remaining() < 2);
                builder = builder.topic_alias_maximum(reader.get_u16())?;
            }
            Property::RequestResponseInformation => {
                end_of_stream!(reader.remaining() < 1);
                builder = builder.request_response_information(reader.get_u8())?;
            },
            Property::RequestProblemInformation => {
                end_of_stream!(reader.remaining() < 1);
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

fn decode_connack_properties(reader: &mut BytesMut) -> Result<ConnAckProperties> {
    let mut builder = PropertiesBuilder::new();
    while (reader.remaining() > 0) {
        let id = decode_variable_integer(reader)?;
        match id.try_into()? {
            Property::SessionExpireInterval => {
                end_of_stream!(reader.remaining() < 4);
                builder = builder.session_expire_interval(reader.get_u32())?;
            },
            Property::ReceiveMaximum => {
                end_of_stream!(reader.remaining() < 2);
                builder = builder.receive_maximum(reader.get_u16())?;
            },
            Property::MaximumQoS => {
                end_of_stream!(reader.remaining() < 1);
                builder = builder.maximum_qos(reader.get_u8())?;
            },
            Property::RetainAvailable => {
                end_of_stream!(reader.remaining() < 1);
                builder = builder.retain_available(reader.get_u8())?;
            },
            Property::MaximumPacketSize => {
                end_of_stream!(reader.remaining() < 4);
                builder = builder.maximum_packet_size(reader.get_u32())?;
            },
            Property::AssignedClientIdentifier => {
                unimplemented!()
            },
            Property::TopicAliasMaximum => {
                end_of_stream!(reader.remaining() < 2);
                builder = builder.topic_alias_maximum(reader.get_u16())?;
            }
            Property::ReasonString => {
                unimplemented!()
            },
            Property::UserProperty => {
                unimplemented!()
            },
            Property::WildcardSubscriptionAvailable => {
                end_of_stream!(reader.remaining() < 1);
                builder = builder.wildcard_subscription_available(reader.get_u8())?;
            },
            Property::SubscriptionIdentifierAvailable => {
                end_of_stream!(reader.remaining() < 1);
                builder = builder.subscription_identifier_available(reader.get_u8())?;
            },
            Property::SharedSubscriptionAvailable => {
                end_of_stream!(reader.remaining() < 1);
                builder = builder.shared_subscription_available(reader.get_u8())?;
            },
            Property::ServerKeepAlive => {
                end_of_stream!(reader.remaining() < 2);
                builder = builder.server_keep_alive(reader.get_u16())?;
            },
            Property::AuthenticationMethod => {
                unimplemented!()
            }
            Property::AuthenticationData => {
                unimplemented!()
            },
            _ => return Err(anyhow!("unknown connect property: {:x}", id))
        }
    }
    Ok(builder.connack())
}

fn decode_variable_integer(reader: &mut BytesMut) -> Result<u32> {
    let mut multiplier = 1;
    let mut value = 0;
    loop {
        if reader.remaining() < 1 { return Err(anyhow!("end of stream")).context("decode_variable_integer") }
        let encoded_byte: u8 = reader.get_u8().into();
        value += (encoded_byte & 0x7F) as u32 * multiplier;
        if multiplier > (0x80 * 0x80 * 0x80) {
            return Err(anyhow!("malformed variable integer: {}", value)).context("decode_variable_integer")
        }
        multiplier *= 0x80;
        if (encoded_byte & 0x80) == 0 {
            break;
        }
    }
    Ok(value)
}

fn decode_utf8_string(reader: &mut BytesMut) -> Result<Bytes> {
    if reader.remaining() >= 2 {
        let len = reader.get_u16() as usize;
        if reader.remaining() >= len {
            Ok(reader.split_to(len).to_bytes())
        } else { Err(anyhow!("end of stream")).context("decode_utf8_string") }
    } else { Err(anyhow!("end of stream")).context("decode_utf8_string") }
}