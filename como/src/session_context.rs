use std::convert::{TryFrom, TryInto};
use std::net::SocketAddr;

use anyhow::Error;
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sled::{IVec, Tree};

use crate::session::Session;

pub(crate) trait SessionContext {
    fn acquire(&self, clean_start: bool) -> Result<Option<SessionState>>;
    fn update(&self, session_state: SessionState) -> Result<()>;
    fn remove(&self) -> Result<()>;
    fn start_monitor(&self);
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionState {
    pub peer: SocketAddr,
    pub expire: Option<i64>,
    pub last_topic_id: Option<u64>,
}

impl SessionState {
    pub fn new(peer: SocketAddr) -> Self {
        SessionState {
            peer,
            expire: None,
            last_topic_id: None,
        }
    }
}

impl TryFrom<IVec> for SessionState {
    type Error = Error;

    fn try_from(encoded: IVec) -> anyhow::Result<Self> {
        bincode::deserialize(encoded.as_ref()).map_err(Error::msg)
    }
}

impl TryFrom<&[u8]> for SessionState {
    type Error = Error;

    fn try_from(encoded: &[u8]) -> anyhow::Result<Self> {
        bincode::deserialize(encoded.as_ref()).map_err(Error::msg)
    }
}

impl TryInto<Vec<u8>> for SessionState {
    type Error = Error;

    fn try_into(self) -> anyhow::Result<Vec<u8>> {
        bincode::serialize(&self).map_err(Error::msg)
    }
}
