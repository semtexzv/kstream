use rdkafka::*;
use rdkafka::config::RDKafkaLogLevel;
use std::fmt;
use serde::export::Formatter;

#[derive(Clone)]
pub struct Config(pub ClientConfig);

impl Config {
    pub fn new() -> Config {
        Config(ClientConfig::new())
    }

    pub fn set(mut self, key: &str, value: &str) -> Config {
        self.0.set(key, value);
        self
    }

    pub fn set_group(mut self, value: &str) -> Self {
        self.0.set("group.id", value);
        self
    }

    pub fn set_app_id(mut self, value: &str) -> Self {
        self
    }


    pub fn set_log_level(mut self, log_level: RDKafkaLogLevel) -> Config {
        self.0.set_log_level(log_level);
        self
    }
}


impl fmt::Debug for Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        return f.debug_struct("Config").finish();
    }
}

