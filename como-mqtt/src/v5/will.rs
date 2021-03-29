use std::convert::{TryFrom, TryInto};
use std::mem::size_of_val;

use anyhow::{anyhow, bail, ensure, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::Encoder;

use crate::v5::decoder::{decode_utf8_string, decode_variable_integer};
use crate::v5::encoder::encode_utf8_string;
use crate::v5::property::{PropertiesBuilder, PropertiesSize, Property, WillProperties};
use crate::v5::types::{MqttCodec, Will};

impl TryFrom<Bytes> for WillProperties {
    type Error = anyhow::Error;

    fn try_from(mut reader: Bytes) -> Result<Self, Self::Error> {
        let mut builder = PropertiesBuilder::default();
        while reader.has_remaining() {
            let id = decode_variable_integer(&mut reader)?;
            match id.try_into()? {
                Property::WillDelayInterval => {
                    end_of_stream!(reader.remaining() < 4, "will delay interval");
                    builder = builder
                        .will_delay_interval(reader.get_u32())
                        .context("will_delay_interval")?;
                }
                Property::PayloadFormatIndicator => {
                    end_of_stream!(reader.remaining() < 1, "will payload format indicator");
                    builder = builder
                        .payload_format_indicator(reader.get_u8())
                        .context("payload_format_indicator")?;
                }
                Property::MessageExpireInterval => {
                    end_of_stream!(reader.remaining() < 4, "will message expire interval");
                    builder = builder
                        .message_expire_interval(reader.get_u32())
                        .context("message_expire_interval")?;
                }
                Property::ContentType => {
                    builder = builder
                        .content_type(
                            decode_utf8_string(&mut reader)
                                .context("decode_utf8_string content_type")?,
                        )
                        .context("content_type")?;
                }
                Property::ResponseTopic => {
                    builder = builder
                        .response_topic(
                            decode_utf8_string(&mut reader)
                                .context("decode_utf8_string response_topic")?,
                        )
                        .context("response_topic")?;
                }
                Property::CorrelationData => unimplemented!(),
                Property::UserProperty => {
                    let user_property = (
                        decode_utf8_string(&mut reader)?,
                        decode_utf8_string(&mut reader)?,
                    );
                    if let (Some(key), Some(value)) = user_property {
                        builder = builder.user_properties((key, value));
                    }
                }
                _ => bail!("unknown will property: {:x}", id),
            }
        }
        Ok(builder.will())
    }
}

impl PropertiesSize for Will {
    fn size(&self) -> usize {
        let properties_length = self.properties.size();
        properties_length.size() + properties_length + self.topic.len() + 2 + self.payload.len()
    }
}

impl Encoder<Will> for MqttCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, msg: Will, writer: &mut BytesMut) -> Result<(), Self::Error> {
        self.encode(msg.properties, writer)?;
        self.encode(msg.topic, writer)?;
        self.encode(msg.payload, writer)
    }
}

impl PropertiesSize for WillProperties {
    fn size(&self) -> usize {
        let mut len = 4 + 1; //will_delay_interval
        len += check_size_of!(self, payload_format_indicator);
        len += check_size_of!(self, message_expire_interval);
        len += check_size_of_string!(self, content_type);
        len += check_size_of_string!(self, response_topic);
        len += check_size_of_string!(self, correlation_data);
        len += self
            .user_properties
            .iter()
            .map(|(x, y)| 5 + x.len() + y.len())
            .sum::<usize>();
        len
    }
}

impl Encoder<WillProperties> for MqttCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        properties: WillProperties,
        writer: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        self.encode(properties.size(), writer)?;
        encode_property_u32!(
            writer,
            WillDelayInterval,
            Some(properties.will_delay_interval)
        );
        encode_property_u8!(
            writer,
            PayloadFormatIndicator,
            properties.payload_format_indicator.map(|b| b as u8)
        );
        encode_property_u32!(
            writer,
            MessageExpireInterval,
            properties.message_expire_interval
        );
        encode_property_string!(writer, ContentType, properties.content_type);
        encode_property_string!(writer, ResponseTopic, properties.response_topic);
        encode_property_string!(writer, CorrelationData, properties.correlation_data);
        encode_property_user_properties!(writer, UserProperty, properties.user_properties);
        Ok(())
    }
}
