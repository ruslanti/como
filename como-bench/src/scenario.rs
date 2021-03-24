use config::{Config, ConfigError, File};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Publication {
    pub clients: usize,
    pub rate: usize,
    pub topic_name: String,
    pub payload_size: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Subscription {
    pub clients: usize,
    pub topic_filter: String,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Scenario {
    pub address: String,
    pub publication: Publication,
    pub subscription: Subscription,
}

impl Default for Publication {
    fn default() -> Self {
        Publication {
            clients: 10,
            rate: 0,
            topic_name: "/topics".to_string(),
            payload_size: 128,
        }
    }
}

impl Default for Subscription {
    fn default() -> Self {
        Subscription {
            clients: 1,
            topic_filter: "#".to_string(),
        }
    }
}

impl Default for Scenario {
    fn default() -> Self {
        Scenario {
            address: "127.0.0.1:1883".to_string(),
            publication: Default::default(),
            subscription: Default::default(),
        }
    }
}

impl Scenario {
    pub fn new() -> Result<Self, ConfigError> {
        let mut cfg = Config::new();
        cfg.merge(File::with_name("scenario"))?;
        cfg.try_into()
    }
}
