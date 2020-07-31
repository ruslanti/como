use config::{ConfigError, Config, File};
use serde::Deserialize;
use crate::mqtt::proto::types::QoS;

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct ServiceSettings {
    pub listen: String,
    pub port: u16,
    pub max_connections: usize
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
pub struct ConnectionSettings {
    pub session_expire_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_qos: Option<QoS>,
    pub retain_available: Option<bool>,
    pub maximum_packet_size: Option<u32>,
    pub topic_alias_maximum: Option<u16>,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub service: ServiceSettings,
    pub connection: ConnectionSettings
}

impl Default for ServiceSettings {
    fn default() -> Self {
        ServiceSettings {
            listen: String::from("127.0.0.1"),
            port: 1883,
            max_connections: 255
        }
    }
}

impl Default for ConnectionSettings {
    fn default() -> Self {
        ConnectionSettings{
            session_expire_interval: None,
            receive_maximum: None,
            maximum_qos: None,
            retain_available: None,
            maximum_packet_size: None,
            topic_alias_maximum: None
        }
    }
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            service: Default::default(),
            connection: Default::default()
        }
    }
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut cfg = Config::new();
        cfg.merge(File::with_name("config/como"))?;
        cfg.try_into()
    }
}