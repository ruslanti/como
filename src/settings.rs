use config::{Config, ConfigError, File};
use serde::Deserialize;

use crate::mqtt::proto::types::QoS;

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct ServiceSettings {
    pub bind: String,
    pub port: u16,
    pub max_connections: usize,
    pub tls: Option<TlsSettings>,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct TlsSettings {
    pub bind: String,
    pub port: u16,
    pub cert: String,
    pub pass: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
pub struct ConnectionSettings {
    pub idle_keep_alive: u16,
    pub server_keep_alive: Option<u16>,
    pub session_expire_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_qos: Option<QoS>,
    pub retain_available: Option<bool>,
    pub maximum_packet_size: Option<u32>,
    pub topic_alias_maximum: Option<u16>,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct LogSettings {
    pub file: Option<String>,
    pub level: String,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub service: ServiceSettings,
    pub connection: ConnectionSettings,
    pub log: LogSettings,
}

impl Default for ServiceSettings {
    fn default() -> Self {
        ServiceSettings {
            bind: String::from("127.0.0.1"),
            port: 1883,
            max_connections: 255,
            tls: None,
        }
    }
}

impl Default for TlsSettings {
    fn default() -> Self {
        TlsSettings {
            bind: String::from("127.0.0.1"),
            port: 8883,
            cert: "".to_string(),
            pass: "".to_string(),
        }
    }
}

impl Default for ConnectionSettings {
    fn default() -> Self {
        ConnectionSettings {
            idle_keep_alive: 500,
            server_keep_alive: None,
            session_expire_interval: None,
            receive_maximum: None,
            maximum_qos: None,
            retain_available: None,
            maximum_packet_size: None,
            topic_alias_maximum: None,
        }
    }
}

impl Default for LogSettings {
    fn default() -> Self {
        LogSettings { file: None, level: "debug".to_string() }
    }
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            service: Default::default(),
            connection: Default::default(),
            log: Default::default(),
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
