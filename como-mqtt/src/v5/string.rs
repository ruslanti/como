use std::convert::TryInto;
use std::ops::Deref;
use std::string::FromUtf8Error;

use bytes::{Buf, Bytes};

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
