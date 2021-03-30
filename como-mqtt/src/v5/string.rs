use std::convert::TryInto;
use std::ops::Deref;
use std::string::FromUtf8Error;

use crate::v5::error::MqttError;
use crate::v5::error::MqttError::EndOfStream;
use crate::v5::types::MqttCodec;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::Encoder;

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct MqttString(Bytes);

impl From<&'static str> for MqttString {
    #[inline]
    fn from(slice: &'static str) -> Self {
        Self(Bytes::from_static(slice.as_bytes()))
    }
}

impl From<String> for MqttString {
    #[inline]
    fn from(s: String) -> Self {
        Self(Bytes::from(s))
    }
}

impl From<Bytes> for MqttString {
    #[inline]
    fn from(b: Bytes) -> Self {
        Self(b)
    }
}

impl TryInto<String> for MqttString {
    type Error = FromUtf8Error;

    fn try_into(self) -> Result<String, Self::Error> {
        String::from_utf8(self.0.to_vec())
    }
}

impl Deref for MqttString {
    type Target = Bytes;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for MqttString {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Buf for MqttString {
    #[inline]
    fn remaining(&self) -> usize {
        self.0.remaining()
    }
    #[inline]
    fn chunk(&self) -> &[u8] {
        self.0.chunk()
    }
    #[inline]
    fn advance(&mut self, cnt: usize) {
        self.0.advance(cnt)
    }
}

impl MqttString {
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Encoder<MqttString> for MqttCodec {
    type Error = MqttError;

    fn encode(&mut self, s: MqttString, writer: &mut BytesMut) -> Result<(), Self::Error> {
        end_of_stream!(writer.capacity() < 2, "encode utf2 string len");
        writer.put_u16(s.len() as u16);
        end_of_stream!(writer.capacity() < s.len(), "encode utf2 string");
        writer.put(s);
        Ok(())
    }
}
