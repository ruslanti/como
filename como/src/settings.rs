use config::{Config, ConfigError, File};
use serde::Deserialize;

use como_mqtt::v5::types::QoS;

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Transport {
    pub bind: String,
    pub port: u16,
    pub max_connections: usize,
    pub tls: Option<Tls>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Tls {
    pub bind: String,
    pub port: u16,
    pub cert: String,
    pub pass: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Connection {
    pub idle_keep_alive: u16,
    pub server_keep_alive: Option<u16>,
    pub session_expire_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_qos: Option<QoS>,
    pub retain_available: Option<bool>,
    pub maximum_packet_size: Option<u32>,
    pub topic_alias_maximum: Option<u16>,
    pub db_path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Logger {
    pub file: Option<String>,
    pub level: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Topics {
    pub temporary: bool,
    pub db_path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub topics: Topics,
    pub service: Transport,
    pub connection: Connection,
    pub log: Logger,
}

impl Default for Topics {
    fn default() -> Self {
        Topics {
            temporary: true,
            db_path: None,
        }
    }
}

impl Default for Transport {
    fn default() -> Self {
        Transport {
            bind: String::from("127.0.0.1"),
            port: 1883,
            max_connections: 255,
            tls: None,
        }
    }
}

impl Default for Tls {
    fn default() -> Self {
        Tls {
            bind: String::from("127.0.0.1"),
            port: 8883,
            cert: "".to_string(),
            pass: "".to_string(),
        }
    }
}

impl Default for Connection {
    fn default() -> Self {
        Connection {
            idle_keep_alive: 500,
            server_keep_alive: None,
            session_expire_interval: None,
            receive_maximum: None,
            maximum_qos: None,
            retain_available: None,
            maximum_packet_size: None,
            topic_alias_maximum: None,
            db_path: None,
        }
    }
}

impl Default for Logger {
    fn default() -> Self {
        Logger {
            file: None,
            level: "debug".to_string(),
        }
    }
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            topics: Default::default(),
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
